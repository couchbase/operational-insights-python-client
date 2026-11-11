=========================
Server Async Request API
=========================

.. contents::
    :local:

.. module:: acouchbase_operational_insights.query_handle

Moving a QueryHandle between processes
++++++++++++++++++++++++++++++++++++++

.. note::
    :meth:`AsyncQueryHandle.to_serialized` and :meth:`AsyncQueryHandle.from_serialized` are **VOLATILE** and subject to change at any time.

A query can be started in one process and continued in another, such as another web worker, a job queue worker or
the same application after a restart.  Serialize the QueryHandle with :meth:`AsyncQueryHandle.to_serialized`, store or send the
string, and deserialize it with :meth:`AsyncQueryHandle.from_serialized` and a :class:`~acouchbase_operational_insights.cluster.AsyncCluster`.
Treat the serialized form as opaque.

.. code-block:: python

    # in the process that starts the query
    serialized = (await cluster.start_query(statement)).to_serialized()

    # in the process that continues it
    handle = AsyncQueryHandle.from_serialized(cluster, serialized)
    status = await handle.fetch_status()

The handle does not depend on the scope that started the query, so a handle from a scope is deserialized with a
cluster.  The server ties a query to the user who started it, so the cluster must use that user's credentials.
With another user's credentials, :meth:`AsyncQueryHandle.fetch_status` raises
:class:`~acouchbase_operational_insights.errors.QueryNotFoundError`.

AsyncQueryHandle
+++++++++++++++++++

.. py:class:: AsyncQueryHandle

    .. automethod:: fetch_status
    .. automethod:: cancel
    .. automethod:: to_serialized
    .. automethod:: from_serialized

AsyncQueryResultHandle
++++++++++++++++++++++++

.. py:class:: AsyncQueryResultHandle

    .. automethod:: fetch_results
    .. automethod:: discard_results

AsyncQueryStatus
+++++++++++++++++++

.. py:class:: AsyncQueryStatus

    .. automethod:: results_ready
    .. automethod:: result_handle
