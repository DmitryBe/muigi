import logging
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
import multiprocessing
import threading

logger = logging.getLogger('luigi-interface')


class Conf(object):
    def __init__(self, mesos_url,
                 docker_image, cmd,
                 cpus, mem,
                 framework_name='Luigy Task',
                 task_max_retry=3):

        self.mesos_url = mesos_url
        self.docker_image = docker_image
        self.cmd = cmd
        self.cpus = cpus
        self.mem = mem
        self.framework_name = framework_name
        self.task_max_retry = task_max_retry


class MuigiContext(object):

    scheduler = None
    driver = None
    queue = None
    task_tracker = None

    @classmethod
    def init(self, conf):
        MuigiContext.get_task_queue()
        MuigiContext.get_task_tracker_dict()
        MuigiContext.get_scheduler(conf)

    @classmethod
    def get_task_tracker_dict(self):
        if self.task_tracker is not None:
            return self.task_tracker
        else:
            manager = multiprocessing.Manager()
            self.task_tracker = manager.dict()
            return self.task_tracker

    @classmethod
    def get_task_queue(self):
        if self.queue is not None:
            return self.queue
        else:
            self.queue = multiprocessing.Queue()
            return self.queue

    @classmethod
    def get_scheduler(self, conf=None):
        if self.scheduler is not None:
            logger.debug("Scheduler exist")
            return self.scheduler
        else:
            logger.debug("Scheduler not exist, creating new")
            if conf is None:
                raise Exception('conf is required to create mesos scheduler')

            framework = mesos_pb2.FrameworkInfo()
            framework.user = "" # Have Mesos fill in the current user.
            framework.name = conf.framework_name
            framework.checkpoint = True
            framework.principal = "luigi-task"

            implicit_acknowledgements = 1

            self.scheduler = MultitaskingScheduler(
                conf.docker_image, conf.cmd,
                conf.cpus, conf.mem,
                conf.task_max_retry)

            logger.info("starting mesos driver")

            self.driver = mesos.native.MesosSchedulerDriver(
                    self.scheduler,
                    framework,
                    conf.mesos_url,
                    implicit_acknowledgements)

            def _worker():
                logger.debug("Starting driver in background thread")
                status = 0 if self.driver.run() == mesos_pb2.DRIVER_STOPPED else 1
                logger.debug("Mesos framework stoped with status: {}".format(status))
                self.driver.stop()

            t = threading.Thread(target=_worker)
            t.setDaemon(True)
            t.start()

            return self.scheduler

    @classmethod
    def stop(self):
        if self.driver:
            self.driver.stop()