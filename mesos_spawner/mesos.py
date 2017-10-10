import os
import re
import json

from six.moves.http_client import HTTPConnection

PROTO_URI_PATTERN = r"^([a-z]+)://([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)"
URI_PATTERN = r"^//([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)"

class MesosMaster:
    def __init__(self, master_uri="http://127.0.0.1:5050"):
        # TODO: Support Zookeeper URL's
        match = re.match(PROTO_URI_PATTERN, master_uri)
        self.protocol, self.host, self.port = match.group(1, 2, 3)
        self.conn = HTTPConnection(self.host, int(self.port), timeout=60)

    def _send(self, url, method, data=None):
        tries = 0
        while tries < 3:
            self.conn.request(method, url)
            response = self.conn.getresponse()

            if response.status == 307:
                uri = response.getheader("Location")
                match = re.match(URI_PATTERN, uri)
                self.host, self.port = match.group(1, 2)
                self.conn = HTTPConnection(self.host, int(self.port), timeout=60)
                continue

        result = json.loads(response.read())
        return result

    def tasks(self, *args, **kwargs):
        url = '/master/tasks.json'
        if len(kwargs) > 0:
            url += '?' + '&'.join(
                ["{}={}".format(key, value) for key, value in kwargs.items()]
            )
        print(url)
        return self._send(url, 'GET')
