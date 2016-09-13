import luigi
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from apps.simple_scheduler import SimpleScheduler


class MesosTaskConfig(luigi.Config):
    mesos_url = luigi.Parameter(default='localhost:5050')
    docker_image = luigi.Parameter(default='')
    resources_cpus = luigi.Parameter(default='0.5')
    resources_mem = luigi.Parameter(default='128')


def log(msg, severity='INFO'):
    print('{}: {}'.format(severity, msg))


class MesosTask(luigi.Task):

    config = MesosTaskConfig()
    mesos_url = config.mesos_url
    docker_image = config.docker_image
    resources_cpus = float(config.resources_cpus)
    resources_mem = float(config.resources_mem)

    # override
    def command(self):
        raise NotImplementedError("command not implemented")

    # override
    def env_vars(self):
        return []

    # override
    def on_complete(self):
        pass

    def run(self):
        log("mesos url: {}".format(self.mesos_url))
        log("required resources (cpus/mem): {}/{}".format(self.resources_cpus, self.resources_mem))
        log("docker image: {}".format(self.docker_image))
        cmd = self.command()
        env_vars = self.env_vars()
        log("cmd: {}".format(cmd))
        log("env vars: {}".format(env_vars))

        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "Luigi Task"
        framework.checkpoint = True
        framework.principal = "luigi-task"

        implicit_acknowledgements = 1

        log("starting mesos driver")
        driver = mesos.native.MesosSchedulerDriver(
                SimpleScheduler(self.docker_image, cmd, self.resources_cpus, self.resources_mem, env_vars),
                framework,
                self.mesos_url,
                implicit_acknowledgements)

        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        log("driver stoped with status: {}".format(status))

        # Ensure that the driver process terminates.
        driver.stop()
        self.on_complete()


