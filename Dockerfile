# syntax=docker/dockerfile:1.6
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create group/user first (stable UID/GID are nice in CI)
ARG UID=10001
ARG GID=10001
RUN groupadd -g $GID app && useradd -m -u $UID -g $GID -s /usr/sbin/nologin appuser

WORKDIR /app

# Install deps as root (faster; can switch to user after)
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install gunicorn "uvicorn[standard]"

# Copy source and set ownership
COPY . .
RUN chown -R appuser:app /app

USER appuser
ENV PORT=8000
EXPOSE 8000
CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","-c","gunicorn_conf.py","app.main:app"]