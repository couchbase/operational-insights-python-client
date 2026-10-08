# Couchbase Python Operational Insights FIT Performer

This is the FIT performer for the [Couchbase Python Operational Insights Client](https://github.com/couchbaselabs/operational-insights-python-client).
It is versioned alongside the SDK: the performer in this directory is always built against whatever SDK
source is currently checked out, not a separately pinned version.

## Prerequisites

- Python >= 3.10
- [uv](https://docs.astral.sh/uv/) (or pip, see below)

## Installing dependencies

All commands below are run from this `performer` directory.

```shell
uv sync --frozen
```

The performer depends on `couchbase-operational-insights` via a path source (`path = ".."`), so this
installs the SDK from the repository root in editable mode rather than from PyPI.

With pip instead:

```shell
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -e ..
```

## Generating protoc files

The `*.proto` files are mirrored from [couchbaselabs/fit-protocol](https://github.com/couchbaselabs/fit-protocol)
into `proto/`. To pick up protocol updates, run:

```shell
./scripts/update-protobuf.sh
```

then commit any changes under `proto/`.

Once the `.proto` files are in place, generate the Python bindings into `insights_performer/protocol`
(they are not committed) with:

```shell
UV_PYTHON=$PWD/.venv/bin/python ./scripts/generate-protos.sh
```

## Running

The performer listens on port 8060 and runs in `BLOCKING` mode by default:

```shell
uv run python server.py -m BLOCKING
uv run python server.py -m ASYNC
```

The mode and log level can also come from the `SERVER_MODE` and `LOG_LEVEL` environment variables.

## Docker

Build and run the performer image from the repository root (not from this directory), since the build
needs access to the full SDK source:

```shell
docker build -f performer/Dockerfile -t insights-python-fit-performer .
docker run -p 8060:8060 insights-python-fit-performer
docker run -p 8060:8060 -e SERVER_MODE=ASYNC insights-python-fit-performer
```

CI publishes this image to GHCR as `insights-python-fit-performer` on every push that touches the SDK or
the performer; FIT runs pull it from there.
