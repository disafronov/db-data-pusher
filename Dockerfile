FROM ubuntu:noble-20250619 AS base

# ENVs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC

# Base dependencies
RUN apt-get update && \
    apt-get install -y \
        tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Drop privileges
USER ubuntu:ubuntu

# Change the working directory to the `app` directory
WORKDIR /home/ubuntu/app

##########################

FROM base AS builder

# Install dependencies
RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=.python-version,target=.python-version \
    uv sync --frozen --no-install-project --link-mode=copy --no-editable --no-dev

# Copy the project into the image
COPY ./main.py /home/ubuntu/app/

# Sync the project
RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=.python-version,target=.python-version \
    uv sync --frozen --link-mode=copy --no-editable --no-dev

##########################

FROM base AS runtime

# Use python from UV
COPY --from=builder /home/ubuntu/.local/share/uv/python/ /home/ubuntu/.local/share/uv/python/

# Copy app directory from builder
COPY --from=builder --chown=ubuntu:ubuntu /home/ubuntu/app/ /home/ubuntu/app/

ENV PATH="/home/ubuntu/app/.venv/bin:$PATH"

# Change the working directory to the app directory
WORKDIR /home/ubuntu/app

ENTRYPOINT [ "python3", "main.py" ]
