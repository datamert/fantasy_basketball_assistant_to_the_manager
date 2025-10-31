FROM python:3.11-slim-bullseye
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV MY_DEBUG_FLAG=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl libblas-dev liblapack-dev libgfortran5 libstdc++6 libz-dev \
  && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --upgrade pip wheel setuptools \
  && pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-u", "main.py"]