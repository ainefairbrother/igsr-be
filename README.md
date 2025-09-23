# IGSR Backend (igsr-be)

FastAPI service for the IGSR website that passes FE queries to Elasticsearch and returns a modified JSON to the FE.
[es-py](https://github.com/igsr/es-py) is the toolkit for generating ES indices from the IGSR SQL database. 