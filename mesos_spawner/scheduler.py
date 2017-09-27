import uuid
import logging
import sys

from queue import Queue

from pymesos import MesosSchedulerDriver, Scheduler, encode_data

TASK_CPU = 0.5
TASK_MEM = 256

class JupyterHubScheduler(Scheduler):

    def __init__(self):
        self.queue = Queue(maxsize=0)
        self.notebook_request = None
        self.tasks_running = set()

    def is_task_running(self, task_id):
        if task_id in self.tasks_running:
            return True
        return False

    def add_notebook(self):
        task_id = str(uuid.uuid4())
        self.queue.put({
            'task_id': task_id,
            'user': 'andereggt',
            'cpus': 0.5,
            'mem': 256
        })
        return task_id

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        if len(offers) < 1:
            logging.debug("No offers, moving on.")
            return

        if not self.notebook_request:
            try:
                self.notebook_request = self.queue.get(False)
            except Queue.Empty:
                logging.debug("No notebook requests, ignoring offers.")
                return

        for offer in offers:
            cpus = self.getResource(offer['resources'], 'cpus')
            mem = self.getResource(offer['resources'], 'mem')

            if cpus < self.notebook_request['cpus'] or mem < self.notebook_request['mem']:
                logging.debug(
                    "Offer insufficient, cpus: {} mem: {}".format(
                        cpus, mem
                    )
                )
                continue

            task_id = self.notebook_request['task_id']

            task = {
                'task_id': {
                    'value': task_id
                },
                'agent_id': {
                    'value': offer['agent_id']['value']
                },
                'name': 'notebook-{}'.format(task_id),
                'command': {
                    'value': ' && '.join([
                        "python3 -m virtualenv env",
                        "env/bin/python -m pip install jupyterhub",
                        "env/bin/jupyterhub-singleuser --ip=0.0.0.0"
                    ]),
                    'user': self.notebook_request['user']
                },
                'resources': [
                    {'name': 'cpus', 'type': 'SCALAR', 'scalar': {'value': self.notebook_request['cpus']}},
                    {'name': 'mem', 'type': 'SCALAR', 'scalar': {'value': self.notebook_request['mem']}}
                ]
            }

            logging.debug("Launching task {}".format(task_id))
            driver.launchTasks(offer['id'], [task], filters)
            self.queue.task_done()
            self.notebook_request = None

        def statusUpdate(self, driver, update):
            task_id = update['task_id']['value']

            if update['state'] == 'TASK_RUNNING':
                self.tasks_running.add(task_id)


class TestScheduler(Scheduler):

    def __init__(self, executor):
        self.executor = executor

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        for offer in offers:
            cpus = self.getResource(offer['resources'], 'cpus')
            mem = self.getResource(offer['resources'], 'mem')

            if cpus < TASK_CPU or mem < TASK_MEM:
                continue

            task_id = str(uuid.uuid4())
            data = encode_data("Hello from task {}!".format(task_id).encode('utf-8'))

            task = {
                'task_id': {
                    'value': task_id
                },
                'agent_id': {
                    'value': offer['agent_id']['value']
                },
                'name': 'task {}'.format(task_id),
                #'executor': self.executor,
                'command': {
                    'value': "{} && {} && {} && {} && {}".format(
                        'git clone https://github.com/tanderegg/mesos-spawner',
                        'virtualenv env',
                        'source env/bin/activate',
                        'python -m pip install -r mesos-spawner/requirements.txt',
                        'echo "Hello world!" && sleep 30'
                    )
                },
                'data': data,
                'resources': [
                    {'name': 'cpus', 'type': 'SCALAR', 'scalar': {'value': TASK_CPU}},
                    {'name': 'mem', 'type': 'SCALAR', 'scalar': {'value': TASK_MEM}}
                ]
            }

            driver.launchTasks(offer['id'], [task], filters)

    def getResource(self, resources, name):
        for resource in resources:
            if resource['name'] == name:
                return resource['scalar']['value']
        return 0.0

    def statusUpdate(self, driver, update):
        logging.debug("Status update TID {}: {}".format(
            update['task_id']['value'], update['state']
        ))
