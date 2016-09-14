import logging

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


logger = logging.getLogger('luigi-interface')


class SimpleScheduler(mesos.interface.Scheduler):

    def __init__(self, docker_image, cmd, resources_cpus, resources_mem, env_vars):
        self.docker_image = str(docker_image)
        self.cmd = str(cmd)
        self.resources_cpus = float(resources_cpus)
        self.resources_mem = float(resources_mem)
        self.env_vars = dict(env_vars)

        self.total_tasks = 1
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.taskData = {}
        self.max_retry_times = 3
        self.retry_times = 0

    def registered(self, driver, frameworkId, masterInfo):
        logger.debug("Registered with framework ID {}".format(frameworkId.value))

    def _create_mesos_task(self, slave_id, tid, docker_image, cmd, task_cpus, task_mem, env_vars):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        task.slave_id.value = slave_id
        task.name = "task {}".format(tid)

        task.command.value = cmd
        task.container.type = mesos_pb2.ContainerInfo.DOCKER

        volume =  mesos_pb2.Volume()
        volume.container_path = "/var/log"
        volume.host_path = "/var/log/"
        volume.mode = mesos_pb2.Volume.RW
        task.container.volumes.extend([volume])

        task.container.docker.image = docker_image
        task.container.docker.network = mesos_pb2.ContainerInfo.DockerInfo.HOST
        task.container.docker.force_pull_image = True

        _tmp = []
        for key, val in env_vars.items():
            p1 = mesos_pb2.Parameter()
            p1.key = "env"
            p1.value = "{}={}".format(key, val)
            _tmp.append(p1)
        task.container.docker.parameters.extend(_tmp)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = task_cpus

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = task_mem

        return task

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            logger.debug("Received offer {} with cpus: {} and mem: {}".format(offer.id.value, offerCpus, offerMem))

            remainingCpus = offerCpus
            remainingMem = offerMem

            while self.tasksLaunched < self.total_tasks and \
                remainingCpus >= self.resources_cpus and \
                remainingMem >= self.resources_mem:
                # launch task
                tid = self.tasksLaunched
                self.tasksLaunched += 1

                logger.debug("Launching task {} using offer {}".format(tid, offer.id.value))
                task = self._create_mesos_task(offer.slave_id.value, tid, self.docker_image, self.cmd, self.resources_cpus, self.resources_mem, self.env_vars)

                tasks.append(task)
                self.taskData[task.task_id.value] = offer.slave_id

                remainingCpus -= self.resources_cpus
                remainingMem -= self.resources_mem

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        logger.debug("Task {} is in state {}".format(update.task_id.value, mesos_pb2.TaskState.Name(update.state)))

        slave_id = self.taskData[update.task_id.value]

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1

        if update.state == mesos_pb2.TASK_ERROR:
            self.tasksFinished += 1
            logger.error("Error message: {}".format(update.message))

        # stop driver if all tasks finished
        if self.tasksFinished == self.total_tasks:
            logger.debug("All tasks finished, stopping driver...")
            driver.abort()

        if update.state == mesos_pb2.TASK_LOST or \
            update.state == mesos_pb2.TASK_KILLED or \
            update.state == mesos_pb2.TASK_FAILED:
            # failed or lost task
            if self.retry_times <= self.max_retry_times:
                self.tasksLaunched -= 1
                self.retry_times += 1
                logger.warning("[retry {}] Task {} is in unexpected state {} with message '{}'".format(self.retry_times, update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message))
            else:
                logger.error("Aborting because task {} is in unexpected state {} with message '{}'".format(update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message))
                driver.abort()


