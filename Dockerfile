FROM python:3.13-slim

# Copy uv binary from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /code

# Add virtual environment to PATH
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first (for better caching)
COPY pyproject.toml .python-version uv.lock ./

# Install dependencies strictly from the lockfile
RUN uv sync 

# Copy the script
COPY ingest_data.py .

# Run the script
ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]