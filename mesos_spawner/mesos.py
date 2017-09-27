import os
import re

from six.moves.http_client import HTTPConnection

URI_PATTERN = r"^([a-z]+)://([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)"

class MesosMaster:
    def __init__(self, master_uri="http://127.0.0.1:5050"):
        # TODO: Support Zookeeper URL's
        match = re.match(URI_PATTERN, master_uri)
        self.protocol, self.host, self.port = match.group(0, 1, 2)
        self.conn = HTTPConnection(host, port, timeout=60)

    def tasks(self, params={}):
        url = '/master/tasks.json'
        conn.request()
