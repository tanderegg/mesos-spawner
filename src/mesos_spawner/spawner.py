import socket
from threading import Thread
import logging

from traitlets import Unicode
from textwrap import dedent
from jupyterhub.spawner import Spawner

from mesos_spawner.scheduler import JupyterHubScheduler

class MesosSpawner(Spawner):
    _scheduler = None
    _scheduler_thread = None
    _count = None

    mesos_master_uri = Unicode(
        config=True,
        help=dedent(
            """
            URI of the Mesos Master(s).
            """
        )
    )

    task_id = Unicode()

    @property
    def count(self):
        cls = self.__class__
        if cls._count is None:
            cls._count = 0
        return cls._count

    @property
    def scheduler(self):
        cls = self.__class__
        if cls._scheduler is None:
            framework_info = {
                #'user': self.user.name,
                'name': 'JupyterHubFramework',
                'hostname': socket.gethostname()
            }

            logging.debug("Starting Mesos scheduler...")
            cls._scheduler = MesosSchedulerDriver(
                JupyterHubScheduler(),
                framework_info,
                self.mesos_master
            )

            def run_driver_thread():
                cls._scheduler.run()

            cls._scheduler_thread = Thread(target=run_driver_thread, args=())
            cls._scheduler_thread.start()
        return cls._scheduler

    def start(self):
        task_id = self.scheduler.add_notebook()
        logging.debug("Spawning Jupyter with task id: {}".format(task_id))

        while True:
            if self.scheduler.is_task_running(task_id):
                break
            else:
                time.sleep(1)

        logging.debug("New Jupyter instance started!")

        self.count += 1


    def poll(self):
        # For now, assuming task is running.
        # Might not need to change this, just let scheduler keep it alive?
        return None

    def stop(self):
        logging.debug("Stopping Jupyter instance...")
        self.count -= 1

        if self.count < 1:
            logging.debug("No more instances, stopping scheduler...")
            self.scheduler.stop()

    def load_state(self, state):
        super(MesosSpawner, self).load_state(state)
        self.task_id = state.get('task_id', '')

    def get_state(self):
        state = super(MesosSpawner, self).get_state()
        if self.task_id:
            state['task_id'] = self.task_id
