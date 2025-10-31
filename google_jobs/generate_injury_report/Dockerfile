FROM python:3.11-slim-bullseye

# Environment setup
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV MY_DEBUG_FLAG=1

# Set working directory
WORKDIR /app

# Install system dependencies including OpenJDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    build-essential \
    ca-certificates \
    curl \
    libblas-dev \
    liblapack-dev \
    libgfortran5 \
    libstdc++6 \
    libz-dev \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so jpype can find libjvm.so
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip wheel setuptools \
  && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Entrypoint
CMD ["python", "-u", "main.py"]