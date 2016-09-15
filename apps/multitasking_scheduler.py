import logging

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
import Queue

logger = logging.getLogger('luigi-interface')


TASK_STATUS_UNKNOWN = 'unknown'
TASK_STATUS_ASSIGNED = 'assigned'
TASK_STATUS_RETRY = 'retry'
TASK_STATUS_FINISHED = 'finished'
TASK_STATUS_FAILED = 'failed'


class MultitaskingScheduler(mesos.interface.Scheduler):

    def __init__(self,
                 docker_image, cmd,
                 task_cpus, task_mem,
                 tasks_env_vars,
                 on_complete,
                 max_retry = 3):
        """
        :param docker_image:
        :param cmd: command entry point
        :param total_tasks: total tasks to run
        :param task_cpus: required cpus per task
        :param task_mem: required mem mb per task
        :param tasks_env_vars: [{'task_id': 1, 'env_vars': {env_vars}}]
        :param on_complete ([{task_id: X, status: ''}])
        """
        self.docker_image = str(docker_image)
        self.cmd = str(cmd)

        # task resources requirements
        self.task_cpus = float(task_cpus)
        self.task_mem = float(task_mem)

        # retry when task lost
        self.max_retry_times = max_retry

        # planned tasks
        self.tasks_queue = Queue.Queue()
        for task_i in tasks_env_vars:
            self.tasks_queue.put(task_i)

        # active (running) tasks
        self.active_tasks = {}

        self.on_complete = on_complete

    def registered(self, driver, frameworkId, masterInfo):
        logger.debug("Registered with framework ID {}".format(frameworkId.value))

    def resourceOffers(self, driver, offers):
        for offer in offers:
            mesos_tasks = []

            offered_cpus = 0
            offered_mem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offered_cpus += resource.scalar.value
                elif resource.name == "mem":
                    offered_mem += resource.scalar.value

            logger.debug("Received offer {} with cpus: {} and mem: {}".format(offer.id.value, offered_cpus, offered_mem))

            remaining_cpus = offered_cpus
            remaining_mem = offered_mem
            while remaining_cpus >= self.task_cpus and \
                remaining_mem >= self.task_mem and \
                self.tasks_queue.empty() is False:
                # enough resources and queued tasks

                task_i = self.tasks_queue.get()
                task_id = str(task_i.get('task_id'))
                task_env_vars = task_i.get('env_vars')

                # create mesos task
                logger.debug("Launching task {} with env vars {} using offer {}".format(task_id, task_env_vars, offer.id.value))
                mesos_task = self._create_mesos_task(offer.slave_id.value, task_id, self.docker_image, self.cmd, self.task_cpus, self.task_mem, task_env_vars)
                mesos_tasks.append(mesos_task)

                # track
                # task.task_id.value is str(tid)
                self.active_tasks[task_id] = {
                    'task_id': task_i,
                    'slave_id': offer.slave_id,
                    'failed': 0,
                    'status': TASK_STATUS_ASSIGNED,
                    'task': task_i
                }

                remaining_cpus -= self.task_cpus
                remaining_mem -= self.task_mem

            # accept mesos offer
            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(mesos_tasks)

            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        task_tracking_record = self.active_tasks[task_id]
        slave_id = task_tracking_record.get('slave_id', 'unknown')

        mesos_task_status = mesos_pb2.TaskState.Name(update.state)
        logger.debug("Task {} on slave {} is in state {}".format(task_id, slave_id, mesos_task_status))

        if update.state == mesos_pb2.TASK_FINISHED:
            task_tracking_record['status'] = TASK_STATUS_FINISHED
            pass

        if update.state == mesos_pb2.TASK_ERROR:
            # self.tasksFinished += 1
            logger.error("Error message: {}".format(update.message))
            # self._reschedule_task(task_tracking_record)
            task_tracking_record['status'] = TASK_STATUS_FAILED

        if update.state == mesos_pb2.TASK_LOST or \
            update.state == mesos_pb2.TASK_KILLED or \
            update.state == mesos_pb2.TASK_FAILED:
            # failed or lost task
            if self._reschedule_task(task_tracking_record):
                task_tracking_record['status'] = TASK_STATUS_RETRY
            else:
                logger.error("Aborting because task {} is in unexpected state {} with message '{}'".format(task_id, mesos_task_status, update.message))
                self._on_complete()
                driver.abort()

        if self.tasks_queue.empty() is True \
            and self._has_active_tasks() is False:
            # stop driver if all tasks finished
            logger.debug("All tasks finished, stopping driver...")
            self._on_complete()
            driver.abort()

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

    def _reschedule_task(self, task_tracking_record):
            task_id = task_tracking_record.get('task_id')
            failed = task_tracking_record.get('failed', 0)
            failed += 1
            if failed < self.max_retry_times:
                logger.error("Task {} will be restarted {} times".format(task_id, failed))
                task_tracking_record['failed'] = failed
                task_i = task_tracking_record['task']
                self.tasks_queue.put(task_i)
                return True
            else:
                logger.error("Max retry reached for task {}".format(task_id))
                return False

    def _has_active_tasks(self):
        l = [x for x in self.active_tasks.values() if x.get('status', TASK_STATUS_UNKNOWN) in [TASK_STATUS_ASSIGNED, TASK_STATUS_RETRY]].__len__()
        return l > 0

    def _on_complete(self):
        if self.on_complete and callable(self.on_complete):
            r = [{'task_id': t.get('task_id', 'unknown'), 'status': t.get('status', TASK_STATUS_UNKNOWN)} for t in self.active_tasks.values()]
            self.on_complete(r)
