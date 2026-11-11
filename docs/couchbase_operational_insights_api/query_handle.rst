=========================
Server Async Request API
=========================

.. contents::
    :local:

.. module:: couchbase_operational_insights.query_handle

Moving a QueryHandle between processes
++++++++++++++++++++++++++++++++++++++

.. note::
    :meth:`BlockingQueryHandle.to_serialized` and :meth:`BlockingQueryHandle.from_serialized` are **VOLATILE** and subject to change at any time.

A query can be started in one process and continued in another, such as another web worker, a job queue worker or
the same application after a restart.  Serialize the QueryHandle with :meth:`BlockingQueryHandle.to_serialized`, store or send the
string, and deserialize it with :meth:`BlockingQueryHandle.from_serialized` and a :class:`~couchbase_operational_insights.cluster.Cluster`.
Treat the serialized form as opaque.

.. code-block:: python

    # in the process that starts the query
    serialized = cluster.start_query(statement).to_serialized()

    # in the process that continues it
    handle = BlockingQueryHandle.from_serialized(cluster, serialized)
    status = handle.fetch_status()

The handle does not depend on the scope that started the query, so a handle from a scope is deserialized with a
cluster.  The server ties a query to the user who started it, so the cluster must use that user's credentials.
With another user's credentials, :meth:`BlockingQueryHandle.fetch_status` raises
:class:`~couchbase_operational_insights.errors.QueryNotFoundError`.

BlockingQueryHandle
+++++++++++++++++++

.. py:class:: BlockingQueryHandle

    .. automethod:: fetch_status
    .. automethod:: cancel
    .. automethod:: to_serialized
    .. automethod:: from_serialized

BlockingQueryResultHandle
+++++++++++++++++++++++++

.. py:class:: BlockingQueryResultHandle

    .. automethod:: fetch_results
    .. automethod:: discard_results

BlockingQueryStatus
+++++++++++++++++++

.. py:class:: BlockingQueryStatus

    .. automethod:: results_ready
    .. automethod:: result_handle
