import sys
import getpass
import socket
import signal
import time
from os.path import abspath, join, dirname
from threading import Thread

from pymesos import MesosSchedulerDriver
from mesos_spawner.scheduler import JupyterScheduler

EXECUTOR_CPU = 0.1
EXECUTOR_MEM = 32

def main(master):
    executor = {
        'executor_id': {
            'value': 'JupyterHubExecutor'
        },
        'name': 'JupyterHubExecutor',
        'command': {
            'value': "{} && {} && {} && {} && {}".format(
                'git clone https://github.com/tanderegg/mesos-spawner',
                'virtualenv env',
                'source env/bin/activate',
                'python -m pip install -r mesos-spawner/requirements.txt',
                'python mesos-spawner/mesos_spawner/executor.py'
            )
        },
        'resources': [
            {'name': 'cpus', 'type': 'SCALAR', 'scalar': {'value': EXECUTOR_CPU}},
            {'name': 'mem', 'type': 'SCALAR', 'scalar': {'value': EXECUTOR_MEM}}
        ]
    }

    framework = {
        'user': getpass.getuser(),
        'name': 'JupyterHubFramework',
        'hostname': socket.gethostname()
    }

    driver = MesosSchedulerDriver(
        JupyterScheduler(executor),
        framework,
        master
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print("Scheduler running, Ctrl-C to stop.")

    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
