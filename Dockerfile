# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# ---

# Debian rather than alpine: the Dagster code location shares this image and
# Dagster's dependencies ship manylinux wheels only.
FROM python:3.13-slim-bookworm

WORKDIR /app

# Same ids as the Deployments' securityContext.
RUN groupadd -g 10001 archive && useradd -u 10001 -g archive -M archive

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH" \
    RT_ARCHIVE_FEEDS=/config/feeds.yaml \
    RT_ARCHIVE_DATA_DIR=/data

USER archive

CMD ["gtfs-zone-rt-archive", "record"]
