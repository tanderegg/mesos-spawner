import socket
import logging
from threading import Thread

from traitlets import Unicode, default
from textwrap import dedent
from jupyterhub.spawner import Spawner
from tornado import gen
from pymesos import MesosSchedulerDriver

from mesos_spawner.scheduler import JupyterHubScheduler

class MesosSpawner(Spawner):
    mesos_master_uri = Unicode(
        config=True,
        help=dedent(
            """
            URI of the Mesos Master(s).
            """
        )
    )

    task_id = Unicode()

    _count = None
    @property
    def count(self):
        """
        A static variable to track number of instances spawned.
        """
        cls = self.__class__
        if cls._count is None:
            cls._count = 0
        return cls._count

    @count.setter
    def count(self, value):
        cls = self.__class__
        cls._count = value

    _scheduler_driver = None
    @property
    def scheduler_driver(self):
        cls = self.__class__
        return cls._scheduler_driver

    _scheduler = None
    _scheduler_thread = None
    @property
    def scheduler(self):
        """
        The global instance of the JupyterHubScheduler for
        managing Mesos tasks.
        """
        cls = self.__class__
        if cls._scheduler is None:
            framework_info = {
                'user': self.user.name,
                'name': 'JupyterHubFramework',
                'hostname': socket.gethostname()
            }

            self.log.debug("Starting Mesos scheduler...")

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
        env = super().get_env()
        self.task_id = self.scheduler.add_notebook(env)
        self.log.debug("Spawning Jupyter with task id: {}".format(self.task_id))

        while True:
            if self.scheduler.is_task_running(self.task_id):
                self.log.debug("New Jupyter instance started!")
                self.count = self.count + 1

                ip = self.scheduler.get_task(self.task_id)['ip']
                port = self.scheduler.get_task(self.task_id)['port']

                return (ip, port)
            yield gen.sleep(1)

    @gen.coroutine
    def poll(self):
        # TODO: More robust state checking
        if self.task_id:
            self.log.debug("Poll determined task is running.")
            return None
        else:
            self.log.debug("Poll determined task is not running.")
            return 0

    @gen.coroutine
    def stop(self, now=False):
        self.log.debug("Stopping Jupyter instance...")
        self.count = self.count - 1

        self.scheduler.kill_task(self.scheduler_driver, self.task_id)
        while True:
            try:
                state = self.scheduler.get_task(self.task_id)['state']
            except KeyError:
                self.log.debug("Task {} does not exist, cannot kill.".format(self.task_id))
                return

            if state not in [
                'TASK_RUNNING', 'TASK_STAGING', 'TASK_KILLING'
            ]:
                self.log.debug("Task {} reached state {}".format(self.task_id, state))
                return
            else:
                self.log.debug("Still killing task {}".format(self.task_id))
                yield gen.sleep(1)

        #if self.count < 1:
        #    self.log.debug("No more instances, stopping scheduler...")
        #    if self.scheduler_driver:
        #        self.scheduler_driver.stop()

    def load_state(self, state):
        super(MesosSpawner, self).load_state(state)
        self.task_id = state.get('task_id', '')

    def get_state(self):
        state = super(MesosSpawner, self).get_state()
        if self.task_id:
            state['task_id'] = self.task_id
