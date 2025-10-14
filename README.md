# `igsr-be`: IGSR Backend

FastAPI service for the IGSR website that passes FE queries to Elasticsearch and returns a modified JSON to the FE.
[es-py](https://github.com/igsr/es-py) is the toolkit for generating ES indices from the IGSR SQL database.

Use this guide to run the full stack locally:

- **Elasticsearch** (ES) in Docker
- **Backend API** (this repo) in Docker
- **Frontend** (IGSR website) in Docker

When finished, browse **http://localhost:8080/** — the FE will talk to this BE, which talks to the local ES instance.

## Repos you’ll need

- **Frontend (FE):** branch from PR [igsr/gca_1000genomes_website/pull/68](https://github.com/igsr/gca_1000genomes_website/pull/68)
- **Backend (BE):** [igsr/igsr-be](https://github.com/igsr/igsr-be)
- **Indexing utilities (to create ES indices):** branch from PR [igsr/es-py/pull/2](https://github.com/igsr/es-py/pull/2)

> Use the es-py repo to **load data & create indices** in your local ES instance - [igsr/es-py/pull/2](https://github.com/igsr/es-py/pull/2) README contains instructions for this.

## Prerequisites

- Docker
- Local ports: **9200** (ES), **8000** (API), **8080** (FE)

## 1) Build the images

From each repo (FE & BE), build a local image:

```bash
# Backend
cd igsr-be
docker build --no-cache -t igsr-be .

# Frontend
cd gca_1000genomes_website
docker build --no-cache -t igsr-fe .
```

## 2) Create a shared Docker network

All three containers will discover each other by **name** on this network:

```bash
docker network create igsr || true
```

## 3) Run Elasticsearch

Spin up a local Elasticsearch instance within which our indices will be created using **es-py**.
For local dev, we disable xpack security. See the **es-py README** for more detailed instructions.

```bash
docker pull docker.elastic.co/elasticsearch/elasticsearch:8.17.2
docker run -d --name es01 --network igsr \
  -p 9200:9200 -p 9300:9300 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  docker.elastic.co/elasticsearch/elasticsearch:8.17.2
```

Check that it's working:

```bash
curl -s http://localhost:9200 | jq .
```

## 4) Prepare the Backend `.env`

In the **igsr-be** repo directory create **`.env`**:

```ini
PORT=8000
CORS_ALLOW_ORIGINS=[http://localhost:8080]

# Elasticsearch
# For BE container: talk to ES by its container name on the shared network
ES_HOST=http://es01:9200
```

## 5) Run the back-end

Supply your .env file that you just created.

```bash
docker run --rm --name igsr-be --network igsr \
  -p 8000:8000 \
  -e PORT=8000 \
  --env-file ./.env \
  igsr-be
```

Health checks:

```bash
# API root
curl -i http://localhost:8000/

# Example search (will error until indices exist)
curl -s http://localhost:8000/beta/sample/_search \
  -H 'content-type: application/json' \
  --data '{"query":{"match_all":{}},"size":1}' | jq .
```

## 6) Load Elasticsearch indices

Follow the **es-py** README (repo: [igsr/es-py/pull/2](https://github.com/igsr/es-py/pull/2)) to **create/populate** these indices in local ES:

- `sample`
- `population`
- `superpopulation`
- `file`
- `analysis_group`
- `data_collections`
- `sitemap`

Until these exist, the BE will return errors for searches.
Quick ES sanity check:

```bash
curl -s 'http://localhost:9200/_cat/indices?v'
```

## 7) Run the Frontend

Point FE at the BE by container name (`igsr-be`) on the shared network:

```bash
docker run --rm --name igsr-fe --network igsr \
  -p 8080:80 \
  -e API_BASE="http://igsr-be:8000" \
  igsr-fe
```

Open the site: **http://localhost:8080/**