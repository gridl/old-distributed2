Distributed
===========

Distributed data-local computing.

This is a proof-of-concept library to play with distributed computing in
Python.


Basic Model
-----------

Worker
~~~~~~

Several ``Worker`` processes live on different machines and communicate with each other
over sockets.

Workers execute computations (function + arguments) and serve data (key-value
pairs).

Workers communicate with each other to gather the necessary data to execute
computations.

Center
~~~~~~

A single ``Center`` process maintains metadata on what data lives on what workers.
Workers update this central process to maintain consistency.  Clients connect
to the center to learn about the network.


User Model
----------

One can use this network of workers in a variety of ways.

At a low level you can always communicate to the workers and center directly
over sockets.

At a high level we can build several interfaces.  This repository currently
contains a ``Pool`` abstraction.


Example
-------


.. code-block:: python

   >>> from distributed import Cluster, Pool

   >>> c = Cluster(hosts=['127.0.0.1:8788', '127.0.0.1:8789'],
   ...             center='127.0.0.1:8787')

   >>> p = Pool('127.0.0.1:8787')
   >>> a, b, c = p.map(lambda x: x * 10, [1, 2, 3])
   >>> a.get(), b.get(), c.get()
   (10, 20, 30)

   >>> p.close()
   >>> c.close()


Motivation and Comparison
-------------------------

This work grew out of a need for a nicer distributed computing solution for
dask_.  The existing ``dask.distributed`` library fails in two ways

1.  It is very complex and hard to extend
2.  It does not respect data locality

To address code complexity this library uses ``asyncio`` and generally saner
asynchronous practices generally.

To address data locality we think more about where to execute jobs when we have
many options.  The existing ``Pool`` code does this decently well.


Other, More Mature Projects
---------------------------

This library is not for general use.  See the following projects for
distributed computing:

Several distributed Python solutions exist.  Some notable examples include the
following:

* scoop_
* Pyro_
* dispy_

Additionally there are higher level systems that provide distributed computing

* PySpark_  (Python on Spark)
* mrjob_ (Python on Hadoop MapReduce)
* Disco_

I omit several other excellent projects.  Pull requests are quite welcome to
this part of the README to include other projects.

.. _dask: http://dask.pydata.org/
.. _dispy: http://dispy.sourceforge.net/
.. _scoop: https://github.com/soravux/scoop/
.. _Pyro: http://dispy.sourceforge.net/
.. _Disco: http://discoproject.org/
.. _PySpark: http://spark.apache.org/docs/latest/api/python/
.. _mrjob: https://pythonhosted.org/mrjob/
