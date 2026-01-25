FROM python:3.13-slim

# 1. Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# 2. Copy pyproject.toml
COPY pyproject.toml .

# 3. Install dependencies (creates .venv)
RUN uv sync

# 4. Add virtual environment to PATH (This is the key fix)
ENV PATH="/app/.venv/bin:$PATH"

# 5. Copy code
COPY pipeline.py .

ENTRYPOINT ["python", "pipeline.py"]