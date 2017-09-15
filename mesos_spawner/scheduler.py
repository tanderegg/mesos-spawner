import uuid
import logging
import sys

from pymesos import MesosSchedulerDriver, Scheduler, encode_data

TASK_CPU = 0.1
TASK_MEM = 32

class JupyterScheduler(Scheduler):

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
                'executor': self.executor,
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
