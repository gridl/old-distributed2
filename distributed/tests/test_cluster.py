from time import sleep
import os

from distributed.cluster import Cluster
from distributed.pool import Pool  # todo: get simpler client
from distributed.utils import get_ip

ip = get_ip()

def test_cluster():
    c = Cluster(hosts=[(ip, 8788), (ip, 8789)],
                center=(ip, 8787))
    try:
        pool = Pool('127.0.0.1', 8787)
        for i in range(300):
            if len(pool.ncores) < 2:
                break
            else:
                sleep(0.01)
        assert set(pool.ncores) == set(c.host_ports)
    finally:
        pool.close()
        c.close()
        os.system('pkill -f dworker --signal 9')
        os.system('pkill -f dcenter --signal 9')
