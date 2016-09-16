import logging
import luigi
from apps.muigi.multitasking_scheduler import MuigiContext, Conf
from apps.muigi.mesos_base_task import MesosBaseTask

logger = logging.getLogger('luigi-interface')


class MesosTaskTest(MesosBaseTask):

    mesos_task_id = luigi.IntParameter()
    env_vars = luigi.DictParameter()

    def on_complete(self):
        with self.output().open('w') as f:
            f.write('ok')

    def output(self):
        return luigi.LocalTarget('tmp/%s/_SUCCESS' % self.task_id)


class RootTaskTest(luigi.WrapperTask):

    mesos_url = luigi.Parameter('10.2.95.5:5050')
    docker_image = luigi.Parameter('docker-dev.hli.io/ccm/mock-01:0.0.2')
    docker_command = luigi.Parameter("sh start.sh")
    resources_cpus = luigi.FloatParameter(1)
    resources_mem = luigi.FloatParameter(128)
    framework_name = luigi.Parameter(default='Muigi')
    task_max_retry = luigi.IntParameter(default=5)
    total_tasks = luigi.IntParameter(default=10)

    def requires(self):

        # mesos framework & scheduler config
        conf = Conf(self.mesos_url,
                    self.docker_image, self.docker_command,
                    self.resources_cpus, self.resources_mem,
                    self.framework_name,
                    self.task_max_retry)
        MuigiContext.init(conf)

        yield [MesosTaskTest(env_vars={'SAY_PARAM': 'hello', 'SLEEP_PARAM': 5}, mesos_task_id=i) for i in range(int(self.total_tasks))]

    def run(self):
        logger.debug('Root task is running')

    def on_failure(self, exception):
        MuigiContext.stop()

    def on_success(self):
        MuigiContext.stop()

