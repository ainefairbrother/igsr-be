# syntax=docker/dockerfile:1.6
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# (optional) system deps if you need them; keep minimal
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m appuser
WORKDIR /app

# Install Python deps first (layer cache)
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install gunicorn "uvicorn[standard]"

# Copy app
COPY . .

# Runtime env (override at run/deploy if needed)
ENV PORT=8000
# Example ES config—match your settings module
# ENV ES_URL=http://elasticsearch:9200
# ENV ES_USERNAME=
# ENV ES_PASSWORD=

EXPOSE 8000

# Gunicorn config picks up $PORT
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn_conf.py", "app.main:app"]