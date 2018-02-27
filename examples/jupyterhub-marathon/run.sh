virtualenv -p python3 /tmp/env
/tmp/env/bin/pip install pycurl jupyter jupyterhub
/tmp/env/bin/pip install git+https://github.com/tanderegg/mesos-spawner.git@init#egg=mesos_spawner
npm install configurable-http-proxy
/tmp/env/bin/jupyterhub --ip 0.0.0.0 --port $PORT --debug
