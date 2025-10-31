FROM python:3.11-slim-bullseye

# Environment setup
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip wheel setuptools \
  && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Entrypoint
CMD ["python", "-u", "main.py"]