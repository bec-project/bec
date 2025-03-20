# Base image
FROM python:3.11-slim AS base

# Set working directory
WORKDIR /bec

# Install dependencies
FROM base AS development
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    tmux \
    && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . .

# Install bec components
RUN pip install -e /bec/bec_lib[dev]
RUN pip install -e /bec/bec_server[dev]
RUN pip install -e /bec/bec_ipython_client[dev]

# Get plugin and ophyd devices

# Strip unneeded files for prod image

# No entrypoint, just run the command you want
