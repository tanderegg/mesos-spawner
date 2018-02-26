import uuid
import logging
import sys

from queue import Queue, Empty

from pymesos import MesosSchedulerDriver, Scheduler, encode_data

TASK_CPU = 0.5
TASK_MEM = 128

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
            'user': 'mesagent',
            'cpus': 0.5,
            'mem': 128
        })
        return task_id

    def getResource(self, resources, name):
        for resource in resources:
            if resource['name'] == name:
                if resource['type'] == 'SCALAR':
                    return resource['scalar']['value']
                elif resource['type'] == 'RANGES':
                    res_range = resource['ranges']['range'][0]
                    return range(int(res_range['begin']), int(res_range['end']))
        return 0.0

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        if len(offers) < 1:
            logging.debug("No offers, moving on.")
            return

        logging.debug("Recieved {} offers".format(len(offers)))

        # If there's no active notebook request, get one off the queue,
        # or decline offers.
        if not self.notebook_request:
            logging.debug("No active Notebook request, getting one off the queue")
            try:
                self.notebook_request = self.queue.get(False)
            except Empty:
                # No requested notebooks, so decline the offer by launching
                # 0 tasks.
                for offer in offers:
                    self._declineOffer(driver, offer, filters)
                logging.debug("No notebook requests enqueued, ignoring offers.")
                return

        for offer in offers:
            if self.notebook_request:
                if not self._processOffer(driver, offer, filters):
                    self._declineOffer(driver, offer, filters)
            else:
                self._declineOffer(driver, offer, filters)

    def statusUpdate(self, driver, update):
        task_id = update['task_id']['value']
        logging.debug("Received task update: {} for {}".format(
            update,
            task_id
        ))

        if update['state'] == 'TASK_RUNNING':
            self.tasks_running.add(task_id)

    def _declineOffer(self, driver, offer, filters):
        driver.launchTasks(offer['id'], [], filters)

    def _processOffer(self, driver, offer, filters):
        logging.debug("Offer: {}".format(offer))
        cpus = self.getResource(offer['resources'], 'cpus')
        mem = self.getResource(offer['resources'], 'mem')
        ports = self.getResource(offer['resources'], 'ports')
        logging.debug("Ports: {}".format(ports))

        logging.debug("Processing offer from agent {} for {} cpus and {} mem".format(
            offer['hostname'], cpus, mem
        ))

        if cpus < self.notebook_request['cpus'] or mem < self.notebook_request['mem']:
            logging.debug(
                "Offer insufficient, cpus: {} mem: {}".format(
                    cpus, mem
                )
            )
            return False

        task_id = self.notebook_request['task_id']

        # Create a private /tmp, and install the virtualenv into it
        # to avoid long path issues.
        task = {
            'task_id': {
                'value': task_id
            },
            'agent_id': {
                'value': offer['agent_id']['value']
            },
            'name': 'jupyterhub-{}-{}'.format(self.notebook_request['user'], task_id),
            'command': {
                'value': ' && '.join([
                    "virtualenv -p python3 /tmp/env",
                    "/tmp/env/bin/python -m pip install jupyter jupyterhub",
                    "/tmp/env/bin/jupyterhub-singleuser --debug -y --ip=0.0.0.0 --port $PORT0"
                ]),
                'user': self.notebook_request['user'],
                'environment': {
                    'variables': [
                        {
                            'name': 'JUPYTERHUB_API_TOKEN',
                            'value': '0'
                        },
                        {
                            'name': 'PORT0',
                            'value': str(ports[0])
                        }
                    ]
                },
            },
            'container': {
                'type': 'MESOS',
                'volumes': [
                    {
                        'container_path': '/tmp',
                        'host_path': 'tmp',
                        'mode': 'RW'
                    }
                ]
            },
            'resources': [
                {'name': 'cpus', 'type': 'SCALAR', 'scalar': {'value': self.notebook_request['cpus']}},
                {'name': 'mem', 'type': 'SCALAR', 'scalar': {'value': self.notebook_request['mem']}},
                {'name': 'ports', 'type': 'RANGES', 'ranges': {'range': {'begin': ports[0], 'end': ports[1]}}}
            ]
        }

        logging.debug("Launching task {}".format(task_id))
        driver.launchTasks(offer['id'], [task], filters)
        self.queue.task_done()
        self.notebook_request = None
        return True

class TestScheduler(Scheduler):
    """
    Scheduler for running one and only one 'Hello world' task.
    """
    def __init__(self):
        self.task_started = False
        self.task_id = None

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        if self.task_started:
            return

        logging.debug("Received offers: {}".format(offers))
        for offer in offers:
            logging.debug("Processing offer: {}".format(offer))

            cpus = self.getResource(offer['resources'], 'cpus')
            mem = self.getResource(offer['resources'], 'mem')
            logging.debug("Offer has {} cpus and {} mem".format(cpus, mem))

            if cpus < TASK_CPU or mem < TASK_MEM:
                logging.debug("Offer does not have sufficient resources")
                continue

            self.task_id = str(uuid.uuid4())
            logging.debug("Starting task: {}".format(self.task_id))

            data = encode_data("Hello from task {}!".format(self.task_id).encode('utf-8'))

            task = {
                'task_id': {
                    'value': self.task_id
                },
                'agent_id': {
                    'value': offer['agent_id']['value']
                },
                'name': 'jupyterhub-task-{}'.format(self.task_id),
                'command': {
                    'value': 'while true; do echo "Hello world!"; sleep 5; done',
                    'uris': [
                        {
                            'value': 'https://github.com/tanderegg/mesos-spawner/archive/master.tar.gz'
                        }
                    ]
                        #"{} && {} && {} && {} && {}".format(
                        #'git clone https://github.com/tanderegg/mesos-spawner',
                        #'virtualenv env',
                        #'source env/bin/activate',
                        #'python -m pip install -r mesos-spawner/requirements.txt',
                        #'echo "Hello world!" && sleep 30'
                    #)
                },
                'data': data,
                'resources': [
                    {'name': 'cpus', 'type': 'SCALAR', 'scalar': {'value': TASK_CPU}},
                    {'name': 'mem', 'type': 'SCALAR', 'scalar': {'value': TASK_MEM}}
                ]
            }

            self.task_started = True
            driver.launchTasks(offer['id'], [task], filters)
            break

    def getResource(self, resources, name):
        for resource in resources:
            if resource['name'] == name:
                return resource['scalar']['value']
        return 0.0

    def statusUpdate(self, driver, update):
        logging.debug("Status update TID {}: {}".format(
            update['task_id']['value'], update['state']
        ))

        if 'reason' in update:
            logging.debug("Update reason: {}".format(update['reason']))

        if update['task_id']['value'] == self.task_id:
            if (update['state'] == 'TASK_FAILED' or
               update['state'] == 'TASK_KILLED' or
               update['state'] == 'TASK_ERROR' or
               update['state'] == 'TASK_GONE' or
               update['state'] == 'TASK_LOST'):
               self.task_started = False
               self.task_id = None
