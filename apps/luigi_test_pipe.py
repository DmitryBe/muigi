import luigi
from apps.luigi_mesos_task import MesosTask

class MesosTaskTest(MesosTask):

    mesos_url = luigi.StringParameter('10.2.95.5:5050')
    docker_image = luigi.StringParameter('docker-dev.hli.io/ccm/mock-01:0.0.2')
    docker_command = luigi.StringParameter("sh start.sh")
    resources_cpus = luigi.FloatParameter(0.5)
    resources_mem = luigi.FloatParameter(128)
    id = luigi.Parameter(default='0')
    sleep = luigi.Parameter(default='10')

    env_vars = luigi.DictParameter({'SAY_PARAM': 'hello', 'SLEEP_PARAM':sleep})

    def on_complete(self):
        with self.output().open('w') as f:
            f.write('ok')

    def output(self):
        return luigi.LocalTarget(is_tmp=True)


class RootTaskTest(luigi.WrapperTask):

    n = luigi.IntParameter(default=1)
    def requires(self):
        yield [MesosTaskTest(id = i, sleep=5) for i in range(self.n)]

    def run(self):
        print("root task is running")


