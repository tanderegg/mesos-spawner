#!/usr/bin/env python2.7
from __future__ import print_function

import sys
import time
from threading import Thread

from pymesos import MesosExecutorDriver, Executor, decode_data

class JupyterHubExecutor(Executor):
    """
    May not be necessary
    """
    def launchTask(self, driver, task):
        def run_task(task):
            update = {
                'task_id': {
                    "value": task['task_id']['value']
                },
                'state': 'TASK_RUNNING',
                'timestamp': time.time()
            }
            driver.sendStatusUpdate(update)

            print(decode_data(task['data']), file=sys.stderr)
            time.sleep(30)

            update = {
                'task_id': {
                    'value': task['task_id']['value']
                },
                'state': 'TASK_FINISHED',
                'timestamp': time.time()
            }
            driver.sendStatusUpdate(update)

        thread = Thread(target=run_task, args=(task,))
        thread.start()
