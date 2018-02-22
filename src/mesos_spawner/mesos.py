import logging
import os
import re
import json
import requests

PROTO_URI_PATTERN = r"^([a-z]+)://([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)"
URI_PATTERN = r"^//([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)"

class MesosMaster:
    def __init__(self, uri="http://127.0.0.1:5050"):
        # TODO: Support Zookeeper URL's
        match = re.match(PROTO_URI_PATTERN, uri)
        self.protocol, self.host, self.port = match.group(1, 2, 3)
        #self.conn = HTTPConnection(self.host, int(self.port), timeout=3)

    def _url(self, path):
        return "{}://{}:{}{}".format(self.protocol,
                                     self.host,
                                     self.port,
                                     path)

    def _send(self, path, method, data=None):
        logging.debug("Sending {} to {} via {}".format(data, path, method))
        tries = 0
        while tries < 3:
            logging.debug("Attempt {}".format(tries+1))

            if method == 'GET':
                response = requests.get(self._url(path))

            logging.debug("Response: {}".format(response))

            if response.status_code == 307:
                # This try doesn't count
                tries -= 1

                logging.debug('Redirecting...')
                uri = response.headers['Location']
                match = re.match(URI_PATTERN, uri)
                self.host, self.port = match.group(1, 2)
                #self.conn = HTTPConnection(self.host, int(self.port), timeout=60)
                continue
            elif response.status_code == 200:
                break

            tries += 1

        return response.json()

    def tasks(self, *args, **kwargs):
        path = '/master/tasks.json'
        if len(kwargs) > 0:
            url += '?' + '&'.join(
                ["{}={}".format(key, value) for key, value in kwargs.items()]
            )
        logging.debug(path)
        return self._send(path, 'GET')
