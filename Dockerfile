FROM python:3.12-slim

# Copy uv binary from the official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first for better layer caching
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Install tzdata for Asia/Jakarta timezone
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && cp /usr/share/zoneinfo/Asia/Jakarta /etc/localtime \
    && echo "Asia/Jakarta" > /etc/timezone \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . .

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "main.py"]
