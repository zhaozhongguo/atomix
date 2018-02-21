# Python client for interacting with Atomix clusters.
from logger import Logger
import requests
import json

class AtomixClient(object):
    """Atomix client."""
    def __init__(self, node):
        self.node = node
        self.log = Logger(node.name, Logger.Type.CLIENT)

    @property
    def address(self):
        return 'http://127.0.0.1:{}'.format(self.node.local_port)

    def map(self, name):
        return Map(self, name)

    def set(self, name):
        return Set(self, name)

    def value(self, name):
        return Value(self, name)

    def counter(self, name):
        return Counter(self, name)

    def lock(self, name):
        return Lock(self, name)

    def _format_url(self, *args, **kwargs):
        args = list(args)
        path = args.pop(0)
        return self.address + path.format(*args, **kwargs)

    def _sanitize(self, response):
        if response.status_code == 200 and response.text != '':
            try:
                return json.loads(response.text)
            except:
                return response.text
        return response.status_code

    def get(self, path, headers=None, *args, **kwargs):
        url = self._format_url(path, *args, **kwargs)
        self.log.message("GET {}", url)
        return self._sanitize(requests.get(url, headers=headers))

    def post(self, path, data=None, headers=None, *args, **kwargs):
        url = self._format_url(path, *args, **kwargs)
        self.log.message("POST {}", url)
        return self._sanitize(requests.post(url, data=data, headers=headers))

    def put(self, path, data=None, headers=None, *args, **kwargs):
        url = self._format_url(path, *args, **kwargs)
        self.log.message("PUT {}", url)
        return self._sanitize(requests.put(url, data=data, headers=headers))

    def delete(self, path, headers=None, *args, **kwargs):
        url = self._format_url(path, *args, **kwargs)
        self.log.message("DELETE {}", url)
        return self._sanitize(requests.delete(url, headers=headers))


class Primitive(object):
    """Atomix distributed primitive."""
    def __init__(self, client, name):
        self.client = client
        self.name = name


class Map(Primitive):
    """Map primitive."""
    def __init__(self, client, name):
        super(Map, self).__init__(client, name)

    def get(self, key):
        return self.client.get('/v1/primitives/maps/{name}/{key}', name=self.name, key=key)

    def put(self, key, value):
        return self.client.put('/v1/primitives/maps/{name}/{key}', name=self.name, key=key, value=value, headers={'content-type': 'text/plain'})

    def remove(self, key):
        return self.client.delete('/v1/primitives/maps/{name}/{key}', name=self.name, key=key)

    def size(self):
        return self.client.get('/v1/primitives/maps/{name}/size', name=self.name)

    def clear(self):
        return self.client.delete('/v1/primitives/maps/{name}', name=self.name)


class Set(Primitive):
    """Set primitive."""
    def __init__(self, client, name):
        super(Set, self).__init__(client, name)

    def add(self, item):
        return self.client.put('/v1/primitives/sets/{name}/{value}', name=self.name, value=item)

    def remove(self, item):
        return self.client.delete('/v1/primitives/sets/{name}/{value}', name=self.name, value=item)

    def contains(self, item):
        return self.client.delete('/v1/primitives/sets/{name}/{value}', name=self.name, value=item)

    def size(self):
        return self.client.get('/v1/primitives/sets/{name}/size', name=self.name)

    def clear(self):
        return self.client.delete('/v1/primitives/sets/{name}', name=self.name)


class Value(Primitive):
    """Value primitive."""
    def __init__(self, client, name):
        super(Value, self).__init__(client, name)

    def get(self):
        return self.client.get('/v1/primitives/values/{name}', name=self.name)

    def set(self, value):
        return self.client.put('/v1/primitives/values/{name}', name=self.name, data=value, headers={'content-type': 'text/plain'})

    def compare_and_set(self, expect, update):
        return self.client.post('/v1/primitives/values/{name}/cas', name=self.name, data={'expect': expect, 'update': update}, headers={'content-type': 'application/json'})


class Counter(Primitive):
    """Counter primitive."""
    def __init__(self, client, name):
        super(Counter, self).__init__(client, name)

    def get(self):
        return self.client.get('/v1/primitives/counters/{name}', name=self.name)

    def set(self, value):
        return self.client.put('/v1/primitives/counters/{name}', name=self.name, data=value, headers={'content-type': 'text/plain'})

    def increment(self):
        return self.client.post('/v1/primitives/counters/{name}/inc', name=self.name)


class Lock(Primitive):
    """Lock primitive."""
    def __init__(self, client, name):
        super(Lock, self).__init__(client, name)

    def lock(self):
        return self.client.post('/v1/primitives/locks/{name}', name=self.name)

    def unlock(self):
        return self.client.delete('/v1/primitives/locks/{name}', name=self.name)
