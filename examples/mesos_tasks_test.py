import sys
from mesos_spawner.mesos import MesosMaster

def main(master_uri):
    master = MesosMaster(uri=master_uri)
    print(master.tasks())

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master_uri>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
