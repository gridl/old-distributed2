from time import sleep
import os

from distributed.cluster import Cluster
from distributed.pool import Pool  # todo: get simpler client
from distributed.utils import get_ip

ip = get_ip()
localhost = '127.0.0.1'

def test_cluster():
    c = Cluster(hosts=[(localhost, 8788), (localhost, 8789)],
                center=(localhost, 8787))
    try:
        sleep(0.1)
        pool = Pool('127.0.0.1', 8787)
        for i in range(10):
            if len(pool.ncores) == 2:
                break
            else:
                sleep(0.1)
                pool.sync_center()
        assert set(pool.ncores) == {(ip, 8788), (ip, 8789)}
    finally:
        pool.close()
        c.close()
        os.system('pkill -f dworker --signal 9')
        os.system('pkill -f dcenter --signal 9')
