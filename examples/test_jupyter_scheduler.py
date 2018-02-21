import sys
import getpass
import socket
import signal
import time
from os.path import abspath, join, dirname
from threading import Thread

from pymesos import MesosSchedulerDriver
from mesos_spawner.scheduler import JupyterHubScheduler

EXECUTOR_CPU = 0.1
EXECUTOR_MEM = 32

def main(master):
    framework = {
        'user': 'mesagent', #getpass.getuser(),
        'name': 'JupyterHubFramework',
        'hostname': socket.gethostname()
    }

    driver = MesosSchedulerDriver(
        JupyterHubScheduler(),
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
        print("Usage: {} <mesos_master_uri>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
