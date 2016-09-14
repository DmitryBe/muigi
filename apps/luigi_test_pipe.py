import luigi
from apps.luigi_mesos_task import MesosTask

class MesosTaskTest(MesosTask):

    mesos_url = luigi.Parameter('10.2.95.5:5050')
    docker_image = luigi.Parameter('docker-dev.hli.io/ccm/mock-01:0.0.2')
    docker_command = luigi.Parameter("sh start.sh")
    resources_cpus = luigi.FloatParameter(0.5)
    resources_mem = luigi.FloatParameter(128)
    env_vars = luigi.DictParameter()

    def on_complete(self):
        with self.output().open('w') as f:
            f.write('ok')

    def output(self):
        return luigi.LocalTarget('tmp/%s/_SUCCESS' % self.task_id)


class RootTaskTest(luigi.WrapperTask):

    n = luigi.IntParameter(default=1)
    def requires(self):
        for i in range(self.n):
            yield MesosTaskTest(env_vars={'SAY_PARAM': 'hello', 'SLEEP_PARAM':i})

    def run(self):
        print("root task is running")


