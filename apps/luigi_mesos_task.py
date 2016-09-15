import logging

import luigi

import mesos.native
import mesos.interface
from mesos.interface import mesos_pb2
from apps.multitasking_scheduler import MultitaskingScheduler

logger = logging.getLogger('luigi-interface')


class MesosTask(luigi.WrapperTask):

    mesos_url = luigi.Parameter()
    docker_image = luigi.Parameter()
    docker_command = luigi.Parameter()
    resources_cpus = luigi.FloatParameter()
    resources_mem = luigi.FloatParameter()
    task_max_retry = luigi.IntParameter()

    def get_tasks(self):
        raise Exception("not implemented")

    def on_complete(self, result):
        pass

    def run(self):
        logger.info("mesos url: {}".format(self.mesos_url))
        logger.info("required resources (cpus/mem): {}/{}".format(self.resources_cpus, self.resources_mem))
        logger.info("docker image: {}".format(self.docker_image))
        logger.info("cmd: {}".format(self.docker_command))
        logger.info("env vars: {}".format(dict(self.env_vars)))

        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "Luigi Task"
        framework.checkpoint = True
        framework.principal = "luigi-task"

        implicit_acknowledgements = 1

        scheduler = MultitaskingScheduler(
            self.docker_image, self.docker_command,
            self.resources_cpus, self.resources_mem,
            self.get_tasks(),
            lambda result: self.on_complete(result),
            self.task_max_retry)

        logger.info("starting mesos driver")
        driver = mesos.native.MesosSchedulerDriver(
                scheduler,
                framework,
                self.mesos_url,
                implicit_acknowledgements)

        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        logger.info("driver stoped with status: {}".format(status))

        # Ensure that the driver process terminates.
        driver.stop()
        self.on_complete()