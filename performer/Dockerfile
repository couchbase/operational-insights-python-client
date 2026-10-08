# A Docker image for the FIT performer for the Couchbase Operational Insights Python
# Client.  The build context is the repository root, so build from there with:
#   docker build -f performer/Dockerfile -t insights-python-fit-performer .

FROM python:3.13-slim-bookworm AS base

FROM base AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.20 /uv /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# The SDK derives its version from `git describe`, so the build needs git and the
# repository metadata.  CI checks out shallow, which `git describe` cannot use.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN if [ -d .git ] && [ "$(git rev-parse --is-shallow-repository)" = "true" ]; then \
        echo "Shallow checkout detected, fetching history and tags..."; \
        git fetch --unshallow --tags origin; \
    fi

WORKDIR /app/performer

# The performer depends on the SDK by path, so this installs both the performer's
# dependencies and the SDK from the source tree copied in above.
RUN uv sync --frozen --no-dev

ENV UV_PYTHON=/app/performer/.venv/bin/python
RUN ./scripts/generate-protos.sh

FROM base

ARG SDK=insights-python

LABEL org.opencontainers.image.title="${SDK}-fit-performer" \
      org.opencontainers.image.description="Couchbase Operational Insights Python Client FIT performer (gRPC server)"

WORKDIR /app

COPY --from=builder /app .
RUN rm -rf .git

ARG RUN_MODE=BLOCKING
ENV SERVER_MODE=$RUN_MODE
ENV LOG_LEVEL=INFO
ENV PATH="/app/performer/.venv/bin:$PATH"

WORKDIR /app/performer

EXPOSE 8060

ENTRYPOINT ["python", "server.py"]
