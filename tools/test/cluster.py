from network import Network
from client import AtomixClient
from logger import Logger
from errors import UnknownNetworkError, UnknownNodeError
from ipaddress import IPv4Network
from six.moves import shlex_quote
import shutil
import os
import docker
import socket
from docker.api.client import APIClient
from docker.utils import kwargs_from_env

class Cluster(object):
    """Atomix test cluster."""
    def __init__(self, name):
        self.log = Logger(name, Logger.Type.FRAMEWORK)
        self.name = name
        self.network = Network(name)
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())

    @property
    def path(self):
        """Returns the cluster data path."""
        return os.path.join(os.getcwd(), self.name)

    def node(self, id):
        """Returns the node with the given ID."""
        if isinstance(id, int):
            return self.nodes[id-1]
        else:
            return [node for node in self.nodes if node.name == id].pop()

    @property
    def nodes(self):
        """Returns a list of nodes in the cluster."""
        # Sort the containers by name and then extract the IP address from the container info.
        containers = sorted(self._docker_client.containers.list(all=True, filters={'label': 'cluster={}'.format(self.name)}), key=lambda c: c.name)
        return [Node(c.name, self._docker_api_client.inspect_container(c.name)['NetworkSettings']['Networks'][self.network.name]['IPAddress'], self) for c in containers]

    def setup(self, nodes=3, subnet='172.18.0.0/16', gateway=None):
        """Sets up the cluster."""
        self.log.message("Setting up cluster", self.name)

        # Create an IPv4 network from which to determine hosts.
        hosts = IPv4Network(unicode(subnet)).hosts()

        # If the gateway is not configured then set it from hosts.
        if gateway is None:
            gateway = str(next(hosts))

        # Set up the test network.
        self.network.setup(subnet, gateway)

        # Iterate through nodes and setup containers.
        for n in range(1, nodes + 1):
            Node('%s-%d' % (self.name, n), str(next(hosts)), self).setup()

    def teardown(self):
        """Tears down the cluster."""
        self.log.message("Tearing down cluster", self.name)
        for node in self.nodes:
            try:
                node.teardown()
            except UnknownNodeError, e:
                self.log.error(str(e))
        try:
            self.network.teardown()
        except UnknownNetworkError, e:
            self.log.error(str(e))

    def cleanup(self):
        """Cleans up the cluster data."""
        self.log.message("Cleaning up cluster state", self.name)
        if os.path.exists(self.path):
            shutil.rmtree(self.path)


class Node(object):
    """Atomix test node."""
    def __init__(self, name, ip, cluster):
        self.log = Logger(cluster.name, Logger.Type.FRAMEWORK)
        self.name = name
        self.ip = ip
        self.http_port = 5678
        self.tcp_port = 5679
        self.cluster = cluster
        self.path = os.path.join(self.cluster.path, self.name)
        self.client = AtomixClient(self)
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())

    def _find_open_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port

    @property
    def local_port(self):
        port_bindings = self._docker_api_client.inspect_container(self.docker_container.name)['HostConfig']['PortBindings']
        return port_bindings['{}/tcp'.format(self.http_port)][0]['HostPort']

    @property
    def docker_container(self):
        try:
            return self._docker_client.containers.get(self.name)
        except docker.errors.NotFound:
            raise UnknownNodeError(self.name)

    def setup(self):
        """Sets up the node."""
        args = []
        args.append('%s:%s:%d' % (self.name, self.ip, self.tcp_port))
        args.append('--bootstrap')
        for node in self.cluster.nodes:
            args.append('%s:%s:%d' % (node.name, node.ip, node.tcp_port))

        self.log.message("Running container {}", self.name)
        self._docker_client.containers.run(
            'atomix',
            ' '.join(args),
            name=self.name,
            labels={'cluster': self.cluster.name},
            network=self.cluster.network.name,
            ports={self.http_port: self._find_open_port()},
            detach=True,
            volumes={self.path: {'bind': '/data', 'mode': 'rw'}})

    def run(self, *command):
        """Runs the given command in the container."""
        command = ' '.join([shlex_quote(arg) for arg in command])
        self.log.message("Executing command: {}", self.name, command)
        return self.docker_container.exec_run(command)

    def stop(self):
        """Stops the node."""
        self.log.message("Stopping node {}", self.name)
        self.docker_container.stop()

    def start(self):
        """Starts the node."""
        self.log.message("Starting node {}", self.name)
        self.docker_container.start()

    def kill(self):
        """Kills the node."""
        self.log.message("Killing node {}", self.name)
        self.docker_container.kill()

    def recover(self):
        """Recovers a killed node."""
        self.log.message("Recovering node {}", self.name)
        self.docker_container.start()

    def restart(self):
        """Restarts the node."""
        self.log.message("Restarting node {}", self.name)
        self.docker_container.restart()

    def partition(self, node):
        """Partitions this node from the given node."""
        self.cluster.network.partition(self, node)

    def heal(self, node):
        """Heals a partition between this node and the given node."""
        self.cluster.network.heal(self, node)

    def isolate(self):
        """Isolates this node from all other nodes in the cluster."""
        self.cluster.network.isolate(self)

    def unisolate(self):
        """Unisolates this node from all other nodes in the cluster."""
        self.cluster.network.unisolate(self)

    def delay(self, latency=50, jitter=10, correlation=.75, distribution='normal'):
        """Delays packets to this node."""
        self.cluster.network.delay(self, latency, jitter, correlation, distribution)

    def drop(self, probability=.02, correlation=.25):
        """Drops packets to this node."""
        self.cluster.network.drop(self, probability, correlation)

    def reorder(self, probability=.02, correlation=.5):
        """Reorders packets to this node."""
        self.cluster.network.reorder(self, probability, correlation)

    def duplicate(self, probability=.005, correlation=.05):
        """Duplicates packets to this node."""
        self.cluster.network.duplicate(self, probability, correlation)

    def corrupt(self, probability=.02):
        """Duplicates packets to this node."""
        self.cluster.network.corrupt(self, probability)

    def restore(self):
        """Restores packets to this node to normal order."""
        self.cluster.network.restore(self)

    def teardown(self):
        """Tears down the node."""
        container = self.docker_container
        self.log.message("Stopping container {}", self.name)
        container.stop()
        self.log.message("Removing container {}", self.name)
        container.remove()

    def wait(self):
        """Waits for the node to exit."""
        self.docker_container.wait()

cluster = Cluster('default')

def set_cluster(name):
    global cluster
    cluster = Cluster(name)

def node(id):
    return cluster.node(id)

def nodes():
    return cluster.nodes
