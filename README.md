# Couchbase Python Operational Insights Client
Python client for [Couchbase](https://couchbase.com) Operational Insights.

Python 3.10+ is supported.

>Note: The SDK uses [ijson](https://github.com/ICRAR/ijson) to stream query results.  On a Python release newer than the ones ijson publishes wheels for, ijson installs from source and, unless the [yajl](https://lloyd.github.io/yajl/) 2 library is present when it is built, uses its pure-Python parser.  The SDK still works, but parsing large results is slower.  The SDK logs which ijson backend it is using when it creates its HTTP client, as a warning if it is the pure-Python one.  To get the C backend, install yajl 2 (e.g. `libyajl-dev` on Debian/Ubuntu, `yajl` on Homebrew) and reinstall the SDK with ijson built from source: `python3 -m pip install --force-reinstall --no-binary ijson couchbase-operational-insights`.

The Operational Insights SDK supports static typing.  Currently only [mypy](https://github.com/python/mypy) is supported.  You mileage may vary (YMMV) with the use of other static type checkers (e.g. [pyright](https://github.com/microsoft/pyright)).

# Installing the SDK<a id="installing-the-sdk"></a>

>Note: It is strongly recommended to update pip, setuptools and wheel prior to installing the SDK: `python3 -m pip install --upgrade pip setuptools wheel`

Install the SDK via `pip`:
```console
python3 -m pip install couchbase-operational-insights
```

# Installing the SDK from source

The SDK can be installed from source via pip with the following command.

Install the SDK via `pip`:
```console
python3 -m pip install git+https://github.com/couchbaselabs/operational-insights-python-client.git
```

# Using the SDK<a id="using-the-sdk"></a>

Some more examples are provided in the [examples directory](https://github.com/couchbaselabs/operational-insights-python-client/tree/main/examples).

**Connecting and executing a query**
```python
from couchbase_operational_insights.cluster import Cluster
from couchbase_operational_insights.credential import Credential
from couchbase_operational_insights.options import QueryOptions


def main() -> None:
    # Update this to your cluster
    # IMPORTANT:  The appropriate port needs to be specified. The SDK's default ports are 80 (http) and 443 (https).
    #             If attempting to connect to Capella, the correct ports are most likely to be 8095 (http) and 18095 (https).
    #             Capella example: https://cb.2xg3vwszqgqcrsix.cloud.couchbase.com:18095
    endpoint = 'https://--your-instance--'
    username = 'username'
    pw = 'password'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    cluster = Cluster.create_instance(endpoint, cred)

    # Execute a query and buffer all result rows in client memory.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
    res = cluster.execute_query(statement)
    all_rows = res.get_all_rows()
    for row in all_rows:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a query and process rows as they arrive from server.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country="United States" LIMIT 10;'
    res = cluster.execute_query(statement)
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with positional arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;'
    res = cluster.execute_query(statement, QueryOptions(positional_parameters=['United States', 10]))
    for row in res:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with named arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;'
    res = cluster.execute_query(statement, QueryOptions(named_parameters={'country': 'United States',
                                                                          'limit': 10}))
    for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')


if __name__ == '__main__':
    main()

```

## Using the async API
```python
import asyncio

from acouchbase_operational_insights.cluster import AsyncCluster
from acouchbase_operational_insights.credential import Credential
from acouchbase_operational_insights.options import QueryOptions


async def main() -> None:
    # Update this to your cluster
    # IMPORTANT:  The appropriate port needs to be specified. The SDK's default ports are 80 (http) and 443 (https).
    #             If attempting to connect to Capella, the correct ports are most likely to be 8095 (http) and 18095 (https).
    #             Capella example: https://cb.2xg3vwszqgqcrsix.cloud.couchbase.com:18095
    endpoint = 'https://--your-instance--'
    username = 'username'
    pw = 'password'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    cluster = AsyncCluster.create_instance(endpoint, cred)

    # Execute a query and buffer all result rows in client memory.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
    res = await cluster.execute_query(statement)
    all_rows = await res.get_all_rows()
    # NOTE: all_rows is a list, _do not_ use `async for`
    for row in all_rows:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a query and process rows as they arrive from server.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country="United States" LIMIT 10;'
    res = await cluster.execute_query(statement)
    async for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with positional arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;'
    res = await cluster.execute_query(statement, QueryOptions(positional_parameters=['United States', 10]))
    async for row in res:
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

    # Execute a streaming query with named arguments.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;'
    res = await cluster.execute_query(statement, QueryOptions(named_parameters={'country': 'United States',
                                                                                'limit': 10}))
    async for row in res.rows():
        print(f'Found row: {row}')
    print(f'metadata={res.metadata()}')

if __name__ == '__main__':
    asyncio.run(main())

```
