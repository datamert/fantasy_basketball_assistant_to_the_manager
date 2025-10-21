# Use a slim Python base image
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir nba_api pandas

# Copy your script into the container
COPY generate_gamelog.py /app/generate_gamelog.py
WORKDIR /app

# Run the script
CMD ["python", "generate_gamelog.py"]
