virtualenv -p python3 /tmp/env
/tmp/env/bin/pip install jupyter jupyterhub
echo "file://$MESOS_SANDBOX/mesos-spawner-init"
/tmp/env/bin/pip install mesos_spawner --no-index --find-links "file://$MESOS_SANDBOX/mesos-spawner-init"
npm install configurable-http-proxy
/tmp/env/bin/jupyterhub --ip 0.0.0.0 --port $PORT
