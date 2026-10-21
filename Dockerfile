# Production container: API + embedded worker in one process group.
# Render's free tier allows one web service per app with no separate
# background-worker type, so the DB-polling worker runs alongside uvicorn
# (started by deploy/start.sh). Both are stateless — all state lives in
# Postgres, supplied via DATABASE_URL.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Install dependencies first so code changes don't invalidate the layer.
# The lockfile is committed: --frozen makes the build reproducible.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Application code (package = false in pyproject, so no install step).
COPY app ./app
COPY static ./static
COPY deploy/start.sh ./start.sh
RUN chmod +x ./start.sh

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

EXPOSE 8000
CMD ["./start.sh"]
