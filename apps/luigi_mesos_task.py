import luigi
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from apps.simple_scheduler import SimpleScheduler

def log(msg, severity='INFO'):
    print('{}: {}'.format(severity, msg))


class MesosTask(luigi.Task):

    mesos_url = luigi.Parameter(default='localhost:5050')
    docker_image = luigi.Parameter()
    docker_command = luigi.Parameter()
    resources_cpus = luigi.FloatParameter()
    resources_mem = luigi.FloatParameter()
    env_vars = luigi.DictParameter()

    def run(self):
        log("mesos url: {}".format(self.mesos_url))
        log("required resources (cpus/mem): {}/{}".format(self.resources_cpus, self.resources_mem))
        log("docker image: {}".format(self.docker_image))
        log("cmd: {}".format(self.docker_command))
        log("env vars: {}".format(dict(self.env_vars)))

        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "Luigi Task"
        framework.checkpoint = True
        framework.principal = "luigi-task"

        implicit_acknowledgements = 1

        log("starting mesos driver")
        driver = mesos.native.MesosSchedulerDriver(
                SimpleScheduler(self.docker_image, self.docker_command, self.resources_cpus, self.resources_mem, self.env_vars),
                framework,
                self.mesos_url,
                implicit_acknowledgements)

        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        log("driver stoped with status: {}".format(status))

        # Ensure that the driver process terminates.
        driver.stop()
        self.on_complete()


