import logging
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from apps.muigi.muigi_context import Conf, MuigiContext

logger = logging.getLogger('luigi-interface')


TASK_STATUS_PENDING = 'pending'
TASK_STATUS_STARTED = 'started'
TASK_STATUS_FINISHED = 'finished'
TASK_STATUS_FAILED = 'failed'


class MultitaskingScheduler(mesos.interface.Scheduler):

    def __init__(self,
                 default_docker_image, default_cmd,
                 default_task_cpus, default_task_mem,
                 default_task_max_retry=5):
        """
            start scheduler
        """
        # incoming tasks queue
        self.tasks_queue = MuigiContext.get_task_queue()

        self.default_docker_image = str(default_docker_image)
        self.default_cmd = str(default_cmd)

        # task resources requirements
        self.default_task_cpus = float(default_task_cpus)
        self.default_task_mem = float(default_task_mem)

        # retry when task lost
        self.default_task_max_retry = default_task_max_retry

        # active tasks
        self.task_tracker = MuigiContext.get_task_tracker_dict()

        # should stop scheduler
        self.stop_scheduler = False

    def schedule_task(self, task_id, env_vars):
        """
        schedule task
        :param task_id
        :param env_vars: {'var1': X, ...}
        :return:
        """
        task = {
            'task_id': str(task_id),
            'env_vars': env_vars
        }
        logger.debug("Schedule task {}".format(task))
        self.tasks_queue.put(task)

        self.task_tracker[str(task_id)] = {
            'slave_id': None,
            'failed': 0,
            'task': task,
            'status': TASK_STATUS_PENDING
        }

        return task_id

    def is_task_running(self, task_id):
        """
        return True if task is running
        :param task_id:
        """
        rec = self.task_tracker.get(str(task_id))
        if rec:
            return rec.get('status') in [TASK_STATUS_PENDING, TASK_STATUS_STARTED]
        else:
            return False

    def get_task_error(self, task_id):
        """
        return None if success or error message
        :param task_id:
        """
        rec = self.task_tracker.get(str(task_id))
        if rec:
            if rec.get('status') in [TASK_STATUS_FINISHED]:
                return None
            elif rec.get('status') in [TASK_STATUS_FAILED]:
                err_msg = rec.get('message')
                return "Task error: {}".format(err_msg)
        else:
            return "Task {} not found".format(task_id)

    def stop(self):
        logger.debug("Request to stop scheduler")
        self.stop_scheduler = True

    def registered(self, driver, frameworkId, masterInfo):
        """
        called by mesos when framework is registered
        """
        logger.debug("Registered with framework ID {}".format(frameworkId.value))

    def resourceOffers(self, driver, offers):
        """
        incoming offers
        :param driver:
        :param offers: list of offers
        :return:
        """

        if self.stop_scheduler:
            logger.debug("Stopping scheduler...")
            driver.abort()
            return

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
            while self.tasks_queue.empty() is False and \
                remaining_cpus >= self.default_task_cpus and \
                remaining_mem >= self.default_task_mem:
                # enough resources and queued tasks

                task_i = self.tasks_queue.get()
                task_id = str(task_i.get('task_id'))
                task_env_vars = task_i.get('env_vars')

                # create mesos task
                logger.debug("Launching task {} with env vars {} using offer {}".format(task_id, task_env_vars, offer.id.value))
                mesos_task = self._create_mesos_task(offer.slave_id.value, task_id, self.default_docker_image, self.default_cmd, self.default_task_cpus, self.default_task_mem, task_env_vars)
                mesos_tasks.append(mesos_task)

                # track
                # task.task_id.value is str(tid)
                self.task_tracker[task_id] = {
                    'slave_id': offer.slave_id,
                    'failed': 0,
                    'task': task_i,
                    'status': TASK_STATUS_STARTED
                }

                remaining_cpus -= self.default_task_cpus
                remaining_mem -= self.default_task_mem

            # accept mesos offer
            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(mesos_tasks)

            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        """
        updates
        :param driver:
        :param update:
        :return:
        """
        task_id = update.task_id.value
        task_tracking_record = self.task_tracker.get(str(task_id))
        slave_id = task_tracking_record.get('slave_id', 'unknown')

        mesos_task_status = mesos_pb2.TaskState.Name(update.state)
        logger.debug("Task {} on slave {} is in state {}".format(task_id, slave_id, mesos_task_status))

        if update.state == mesos_pb2.TASK_FINISHED:
            self.task_tracker[str(task_id)] = {
                'status': TASK_STATUS_FINISHED
            }

        if update.state == mesos_pb2.TASK_ERROR:
            logger.error("Error message: {}".format(update.message))
            self.task_tracker[str(task_id)] = {
                'status': TASK_STATUS_FAILED,
                'message': update.message
            }

        if update.state == mesos_pb2.TASK_LOST or \
            update.state == mesos_pb2.TASK_KILLED or \
            update.state == mesos_pb2.TASK_FAILED:
            # failed or lost task
            if self._reschedule_task(task_tracking_record):
                self.task_tracker[str(task_id)] = {
                    'status': TASK_STATUS_PENDING
                }
            else:
                logger.error("Aborting task {} is in unexpected state {} with message '{}'".format(task_id, mesos_task_status, update.message))
                self.task_tracker[str(task_id)] = {
                    'status': TASK_STATUS_FAILED,
                    'message': update.message
                }

    def _create_mesos_task(self, slave_id, tid, docker_image, cmd, task_cpus, task_mem, env_vars):
        """
        create mesos task
        :param slave_id:
        :param tid:
        :param docker_image:
        :param cmd:
        :param task_cpus:
        :param task_mem:
        :param env_vars:
        :return:
        """
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
            if failed < self.default_task_max_retry:
                logger.error("Task {} will be restarted {} times".format(task_id, failed))
                task_tracking_record['failed'] = failed
                task_i = task_tracking_record['task']
                # put task back to queue
                self.tasks_queue.put(task_i)
                return True
            else:
                logger.error("Max retry reached for task {}".format(task_id))
                return False
