import sys
import getpass
import socket
import signal
import time
from os.path import abspath, join, dirname
from threading import Thread

from pymesos import MesosSchedulerDriver
from mesos_spawner.scheduler import JupyterHubScheduler

def main(master):
    framework = {
        'user': 'mesagent', #getpass.getuser(),
        'name': 'JupyterHubFramework',
        'hostname': socket.gethostname()
    }

    scheduler = JupyterHubScheduler()

    driver = MesosSchedulerDriver(
        scheduler,
        framework,
        master
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        logging.debug("Running driver thread")
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print("Scheduler running, Ctrl-C to stop.")

    signal.signal(signal.SIGINT, signal_handler)

    seconds = 0
    while driver_thread.is_alive():
        time.sleep(1)
        seconds += 1
        if seconds == 5:
            task_id = scheduler.add_notebook({})
            logging.debug("Requested new notebook: {}".format(task_id))

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master_uri>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
