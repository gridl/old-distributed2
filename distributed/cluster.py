import paramiko
import asyncio

from .core import rpc, sync

log = print

def normalize_host_port(x, default_port=8787):
    """ Normalize input to host, port pair

    >>> normalize_host_port('127.0.0.1:8000')
    ('127.0.0.1', 8000)
    >>> normalize_host_port('127.0.0.1', default_port=8000)
    ('127.0.0.1', 8000)
    >>> normalize_host_port(('127.0.0.1', 8000))
    ('127.0.0.1', 8000)
    """
    if isinstance(x, (tuple, list)):
        host, port = x
    if isinstance(x, str) and ':' in x:
        host, port = x.split(':')
    if isinstance(x, str) and ':' not in x:
        host, port = x, default_port
    port = int(port)
    return (host, port)


class Cluster(object):
    """ Proxy for a cluster of Workers around a Center

    This starts one ``dcenter`` process and several ``dworker`` processes.

    >>> from distributed import Cluster, Pool

    >>> c = Cluster(hosts=['127.0.0.1:8788', '127.0.0.1:8789'],
    ...             center='127.0.0.1:8787')  # doctest: +SKIP

    Do stuff with the cluster

    >>> p = Pool('127.0.0.1:8787')  # doctest: +SKIP
    >>> a, b, c = p.map(lambda x: x * 10, [1, 2, 3])  # doctest: +SKIP
    >>> a.get(), b.get(), c.get()  # doctest: +SKIP
    (10, 20, 30)
    >>> p.close()  # doctest: +SKIP

    Close the cluster

    >>> c.close()  # doctest: +SKIP
    """
    def __init__(self, hosts, center=None, **auth):
        center = center or hosts[0]
        self.center, self.center_port = normalize_host_port(center, 8787)

        self.host_ports = tuple(normalize_host_port(host, 8787)
                                for host in hosts)
        self.auth = auth

        self.start()

    @property
    def hosts(self):
        """ Just the hosts, not the ports """
        return tuple(host for host, port in self.host_ports)

    def start(self):
        self.remote(self.center, 'nohup /usr/bin/env dcenter &> center.log &')

        command = ('nohup /usr/bin/env dworker %s:%d &> worker.log &' %
                   (self.center, self.center_port))
        for host in self.hosts:
            self.remote(host, command)

    def remote(self, host, command):
        """ Execute command on host

        1.  Create an ssh connection with the stored authentication
        2.  Execute the given command string
        3.  Log stdout and stderr
        4.  Close ssh connection
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, **self.auth)
        log(host, command)
        stdin, stdout, stderr = ssh.exec_command(command)
        log("stdout: %s" % stdout.read())
        log("stderr: %s" % stderr.read())
        ssh.close()

    def close(self, loop=None):
        """ Terminate remote workers and center """
        loop = loop or asyncio.get_event_loop()

        coroutines = [rpc(host, port, loop).terminate() for host, port in
                        self.host_ports]

        sync(asyncio.gather(*coroutines, loop=loop), loop=loop)
        sync(rpc(self.center, self.center_port, loop).terminate(), loop=loop)
