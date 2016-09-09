import luigi
from time import sleep
import os
import sys
import time
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from apps.luigi_mesos_task import MesosTask

class MesosTaskTest(MesosTask):

    docker_image = 'docker-dev.hli.io/ccm/mock-01:0.0.2'
    resources_cpus = 0.5
    resources_mem = 128

    id = luigi.Parameter(default='0')
    sleep = luigi.Parameter(default='10')

    def command(self):
        return "sh start.sh"

    def env_vars(self):
        return ['SAY_PARAM=hello', 'SLEEP_PARAM=%s' % self.sleep]

    def on_complete(self):
        with self.output().open('w') as f:
            f.write('ok')

    def output(self):
        return luigi.LocalTarget('tmp/%s/_SUCCESS' % self.id)


class RootTaskTest(luigi.WrapperTask):

    n = luigi.IntParameter(default=1)
    def requires(self):
        yield [MesosTaskTest(id = i, sleep=5) for i in range(self.n)]

    def run(self):
        print("root task is running")


