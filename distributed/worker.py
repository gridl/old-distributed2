import asyncio
import random
from toolz import merge, partial
from multiprocessing.pool import ThreadPool

from .core import (read, write, connect, delay, client_connected, get_data,
        spawn_loop, rpc, sync)
from . import core
from .client import collect_from_center


_ncores = ThreadPool()._processes

log = print

class Worker(object):
    """ Worker node in a distributed network

    Workers do the following:

    1.  Manage and serve from a dictionary of local data
    2.  Perform computations on that data and on data from peers
    3.  Interact with peers and with a ``Center`` node to acheive 2

    A worker should connect to a ``Center`` node.  It can run in an event loop
    or separately in a thread.

    Example
    -------

    Set up a Center on a separate machine

    >>> c = Center('192.168.0.100', 8000)  # doctest: +SKIP

    Run in an event loop

    >>> w = Worker('192.168.0.101', 8001,
    ...            center_ip='192.168.0.100', center_port=8000) # doctest: +SKIP
    >>> coroutine = w.go()  # doctest: +SKIP

    Can run separately in a thread

    >>> w = Worker('192.168.0.101', 8001,
    ...            center_ip='192.168.0.100', center_port=8000,
    ...            start=True, block=False)  # doctest: +SKIP
    """

    def __init__(self, ip, port, center_ip, center_port, bind='*', loop=None,
                 start=False, block=True, ncores=None):
        self.ip = ip
        self.port = port
        self.center_ip = center_ip
        self.center_port = center_port
        self.bind = bind
        self.loop = loop or asyncio.new_event_loop()
        self.ncores = ncores or _ncores
        self.data = dict()
        self.status = None
        self._log = []

        if start:
            self.start(block)

    @asyncio.coroutine
    def go(self):
        handlers = {'compute': partial(work, self.loop, self.data,
                                             self.ip, self.port,
                                             self.center_ip, self.center_port),
                    'get_data': partial(get_data, self.data),
                    'update_data': partial(update_data, self.loop, self.data,
                                           self.ip, self.port,
                                           self.center_ip, self.center_port),
                    'delete_data': partial(delete_data, self.loop, self.data,
                                           self.ip, self.port,
                                           self.center_ip, self.center_port),
                    'terminate': self._terminate}

        self.server = yield from asyncio.start_server(
                client_connected(handlers), self.bind, self.port,
                loop=self.loop)

        resp = yield from rpc(self.center_ip, self.center_port).register(
                              ncores=self.ncores, address=(self.ip, self.port),
                              loop=self.loop)
        assert resp == b'OK'
        log('Registered with center')

        log('Start worker')
        self.status = 'running'
        yield from self.server.wait_closed()
        log('Stop worker')

    def start(self, block):
        """ Start worker.

        If block is false then run the event loop in a separate thread
        """
        if block:
            try:
                self._go_task = asyncio.Task(self.go(), loop=self.loop)
                self.loop.run_until_complete(self._go_task)
            except KeyboardInterrupt as e:
                self.loop.run_until_complete(self._close())
                self.loop.run_until_complete(self._go_task)
                raise e
        else:
            self._thread, _ = spawn_loop(self.go(), loop=self.loop)

    @asyncio.coroutine
    def _close(self):
        yield from rpc(self.center_ip, self.center_port, loop=self.loop).unregister(
                address=(self.ip, self.port))
        self.server.close()
        self.status = 'closed'

    def close(self):
        result = sync(self._close(), self.loop)
        if hasattr(self, '_thread'):
            self._thread.join()

    @asyncio.coroutine
    def _terminate(self, reader, writer):
        yield from self._close()

    def log(self, *args):
        self._log.append(args)

    @property
    def address(self):
        return (self.ip, self.port)


job_counter = [0]

@asyncio.coroutine
def work(loop, data, ip, port, metadata_ip, metadata_port, reader, writer,
        function=None, key=None, args=(), kwargs={}, needed=[]):
    """ Execute function """
    m_reader, m_writer = yield from connect(metadata_ip, metadata_port,
                                            loop=loop)

    needed = [n for n in needed if n not in data]

    # Collect data from peers
    if needed:
        log("Collect data from peers: %s" % str(needed))
        other = yield from collect_from_center(m_reader, m_writer, needed,
                loop=loop)
        data2 = merge(data, dict(zip(needed, other)))
    else:
        data2 = data

    # Fill args with data, compute in separate thread
    args2 = keys_to_data(args, data2)
    kwargs2 = keys_to_data(kwargs, data2)
    try:
        job_counter[0] += 1
        i = job_counter[0]
        log("Start job %d: %s" % (i, function.__name__))
        result = yield from delay(loop, function, *args2, **kwargs)
        log("Finish job %d: %s" % (i, function.__name__))
        out_response = b'success'
    except Exception as e:
        result = e
        out_response = b'error'
    data[key] = result

    # Tell center about our new data
    response = yield from rpc(m_reader, m_writer).add_keys(
            address=(ip, port), close=True, keys=[key])
    if not response == b'OK':
        log('Could not report results of work to center: ' + response.decode())

    return out_response


def keys_to_data(o, data):
    """ Merge known data into tuple or dict

    >>> data = {'x': 1}
    >>> keys_to_data(('x', 'y'), data)
    (1, 'y')
    >>> keys_to_data({'a': 'x', 'b': 'y'}, data)
    {'a': 1, 'b': 'y'}
    """
    if isinstance(o, (tuple, list)):
        result = []
        for arg in o:
            try:
                result.append(data[arg])
            except (TypeError, KeyError):
                result.append(arg)
        result = type(o)(result)

    if isinstance(o, dict):
        result = {}
        for k, v in o.items():
            try:
                result[k] = data[v]
            except (TypeError, KeyError):
                result[k] = v
    return result


@asyncio.coroutine
def update_data(loop, old_data, ip, port, center_ip, center_port,
                reader, writer, data=None, report=True):
    old_data.update(data)
    if report:
        yield from rpc(center_ip, center_port, loop=loop).add_keys(
                address=(ip, port), keys=list(data))
    return b'OK'


@asyncio.coroutine
def delete_data(loop, data, ip, port, center_ip, center_port, reader, writer,
                keys=None, report=True):
    for key in keys:
        if key in data:
            del data[key]
    if report:
        yield from rpc(center_ip, center_port, loop=loop).remove_keys(
                address=(ip, port), keys=keys)
    return b'OK'
