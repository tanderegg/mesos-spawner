import uuid
import logging
import sys

from queue import Queue, Empty

from pymesos import MesosSchedulerDriver, Scheduler, encode_data

TASK_CPU = 0.5
TASK_MEM = 99

class JupyterHubScheduler(Scheduler):

    def __init__(self):
        """
        The JupyterHubScheduler waits for a request to be added to
        self.request_queue by the MesosSpawner, then sets it as the
        self.current_request and takes it off the queue.  self.tasks_running
        tracks active Jupyterhub sessions, and self.task_info contains metadata.
        """
        self.request_queue = Queue(maxsize=0)
        self.current_request = None
        self.tasks_running = set()
        self.task_info = dict()

    def is_task_running(self, task_id):
        if task_id in self.tasks_running:
            return True
        return False

    def add_notebook(self, env):
        task_id = str(uuid.uuid4())
        self.request_queue.put({
            'task_id': task_id,
            'user': 'mesagent',
            'cpus': TASK_CPU,
            'mem': TASK_MEM,
            'env': env
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

    def get_task(self, task_id):
        return self.task_info[task_id]

    def kill_task(self, driver, task_id):
        if task_id in self.task_info:
            self.task_info[task_id]['state'] == 'TASK_KILLING'
        driver.killTask({"value": task_id})

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        if len(offers) < 1:
            logging.debug("No offers, moving on.")
            return

        logging.debug("Recieved {} offers".format(len(offers)))

        # If there's no active notebook request, get one off the queue,
        # or decline offers.
        if not self.current_request:
            logging.debug("No active Notebook request, getting one off the queue")
            try:
                self.current_request = self.request_queue.get(False)
            except Empty:
                # No requested notebooks, so decline the offer by launching
                # 0 tasks.
                for offer in offers:
                    self._declineOffer(driver, offer, filters)
                logging.debug("No notebook requests enqueued, ignoring offers.")
                return

        for offer in offers:
            if self.current_request:
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

        self.task_info[task_id]['state'] = update['state']

        if update['state'] == 'TASK_RUNNING':
            self.tasks_running.add(task_id)

        if (update['state'] == 'TASK_FAILED' or
           update['state'] == 'TASK_KILLED' or
           update['state'] == 'TASK_ERROR' or
           update['state'] == 'TASK_GONE' or
           update['state'] == 'TASK_LOST'):
           if task_id in self.tasks_running:
               self.tasks_running.remove(task_id)
           else:
               logging.debug("Received task termination message from task that is not running.")

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

        if cpus < self.current_request['cpus'] or mem < self.current_request['mem']:
            logging.debug(
                "Offer insufficient, cpus: {} mem: {}".format(
                    cpus, mem
                )
            )
            return False

        task_id = self.current_request['task_id']

        # Create a private /tmp, and install the virtualenv into it
        # to avoid long path issues.
        task = {
            'task_id': {
                'value': task_id
            },
            'agent_id': {
                'value': offer['agent_id']['value']
            },
            'name': 'jupyterhub-{}-{}'.format(self.current_request['user'], task_id),
            'command': {
                'value': ' && '.join([
                    "env",
                    "virtualenv -p python3 --system-site-packages /tmp/env",
                    "source /tmp/env/bin/activate",
                    "pip install -I jupyter jupyterhub beakerx",
                    "sed -i 's/, exist_ok=True//g' /tmp/env/lib/python3.4/site-packages/beakerx/install.py",
                    "beakerx install",
                    "jupyter nbextension enable beakerx --py --sys-prefix",
                    "jupyterhub-singleuser --cookie-name=$JPY_COOKIE_NAME --debug -y --ip=0.0.0.0 --port $PORT0 --user {}".format(self.current_request['user'])
                ]),
                'user': self.current_request['user'],
                'environment': {
                    'variables': [
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
                {'name': 'cpus', 'type': 'SCALAR', 'scalar': {'value': self.current_request['cpus']}},
                {'name': 'mem', 'type': 'SCALAR', 'scalar': {'value': self.current_request['mem']}},
                {'name': 'ports', 'type': 'RANGES', 'ranges': {'range': {'begin': ports[0], 'end': ports[1]}}}
            ]
        }

        for key, value in self.current_request['env'].items():
            task['command']['environment']['variables'].append({
                'name': key,
                'value': value
            })

        logging.debug("Launching task {}".format(task_id))
        driver.launchTasks(offer['id'], [task], filters)
        self.task_info[task_id] = {
            "port": ports[0],
            "ip": offer['url']['address']['ip'],
            "state": "TASK_STAGING"
        }
        self.request_queue.task_done()
        self.current_request = None
        return True
