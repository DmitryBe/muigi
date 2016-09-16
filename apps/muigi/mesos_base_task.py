import logging
from time import sleep
import luigi

from apps.muigi.multitasking_scheduler import MuigiContext, Conf

logger = logging.getLogger('luigi-interface')


class MesosBaseTask(luigi.Task):

    sleep_sec = luigi.IntParameter(default=1)
    mesos_task_id = luigi.IntParameter()
    env_vars = luigi.DictParameter()

    def on_complete(self):
        pass

    def run(self):

        scheduler = MuigiContext.get_scheduler()
        scheduler.schedule_task(task_id=self.mesos_task_id, env_vars=self.env_vars)
        while scheduler.is_task_running(self.mesos_task_id):
            sleep(self.sleep_sec)

        err = scheduler.get_task_error(self.mesos_task_id)
        if err:
            raise Exception("Task {} failed with message: {}".format(self.mesos_task_id, err))
        else:
            self.on_complete()
