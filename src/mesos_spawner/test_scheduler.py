from pymesos import MesosSchedulerDriver, Scheduler, encode_data

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
