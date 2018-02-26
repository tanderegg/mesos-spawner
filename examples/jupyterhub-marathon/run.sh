virtualenv -p python3 /tmp/env
/tmp/env/bin/pip install jupyter jupyterhub
npm install configurable-http-proxy
/tmp/env/bin/jupyterhub --ip 0.0.0.0 --port $PORT
