# Use minimal Python image
FROM python:3.11-slim

# Prevent Python from writing pyc files & buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install minimal system dependencies for Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first (Docker layer caching)
COPY requirements.txt .

# Upgrade pip & install dependencies without cache
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Create non-root user & switch
RUN useradd -m appuser
USER appuser

# Expose port
EXPOSE 10000

# Run FastAPI with Gunicorn + Uvicorn workers
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "4", "-b", "0.0.0.0:10000", "main:app"]