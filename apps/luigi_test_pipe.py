import luigi
from apps.luigi_mesos_task import MesosTask


class MesosTaskTest(MesosTask):

    mesos_url = luigi.Parameter('10.2.95.5:5050')
    docker_image = luigi.Parameter('docker-dev.hli.io/ccm/mock-01:0.0.2')
    docker_command = luigi.Parameter("sh start.sh")
    resources_cpus = luigi.FloatParameter(0.5)
    resources_mem = luigi.FloatParameter(128)
    task_max_retry = luigi.IntParameter(default=3)
    mesos_tasks = luigi.IntParameter(default=5)

    def get_tasks(self):
        _tasks = []
        for i in range(int(self.mesos_tasks)):
            _tasks.append({
                'task_id': i,
                'env_vars': {'SAY_PARAM': 'hello', 'SLEEP_PARAM': i}
            })
        return _tasks

    def on_complete(self, result):

        failed_count = [x for x in result if x.get('status') == 'failed'].__len__()
        if failed_count == 0:
            with self.output().open('w') as f:
                f.write('ok')
        else:
            raise Exception('failed tasks: {}'.format(failed_count))

    def output(self):
        return luigi.LocalTarget('tmp/%s/_SUCCESS' % self.task_id)


class RootTaskTest(luigi.WrapperTask):

    mesos_tasks = luigi.IntParameter(default=1)

    def requires(self):
        return MesosTaskTest(mesos_tasks=self.mesos_tasks)

    def run(self):
        print("root task is running")


