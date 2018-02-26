import socket
import logging
from threading import Thread

from traitlets import Unicode
from textwrap import dedent
from jupyterhub.spawner import Spawner
from tornado import gen
from pymesos import MesosSchedulerDriver

from mesos_spawner.scheduler import JupyterHubScheduler

class MesosSpawner(Spawner):
    _scheduler = None
    _scheduler_thread = None
    _scheduler_driver = None
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
                'user': 'mesagent',
                'name': 'JupyterHubFramework',
                'hostname': socket.gethostname()
            }

            logging.debug("Starting Mesos scheduler...")
            cls._scheduler = JupyterHubScheduler()
            cls._scheduler_driver = MesosSchedulerDriver(
                self._scheduler,
                framework_info,
                self.mesos_master_uri
            )

            def run_driver_thread():
                cls._scheduler_driver.run()

            cls._scheduler_thread = Thread(target=run_driver_thread, args=())
            cls._scheduler_thread.start()
        return cls._scheduler

    @gen.coroutine
    def start(self):
        task_id = self.scheduler.add_notebook()
        logging.debug("Spawning Jupyter with task id: {}".format(task_id))

        while True:
            if self.scheduler.is_task_running(task_id):
                logging.debug("New Jupyter instance started!")
                self.count = self.count + 1

                # TODO: Get real ones
                ip = "10.0.1.34"
                port = 1234

                return (ip, port)
            yield gen.sleep(1)

    def poll(self):
        # TODO: More robust state checking
        if self.task_id:
            return None
        else:
            return 0

    def stop(self):
        logging.debug("Stopping Jupyter instance...")
        self.count = self.count - 1

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
