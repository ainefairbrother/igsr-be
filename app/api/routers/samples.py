from fastapi import APIRouter, Query
from elasticsearch import NotFoundError
from elasticsearch import TransportError
from app.services.es import es  # your shared ES client

router = APIRouter()

# The ES "shape" your FE expects when there are zero results.
ES_SHAPE_EMPTY = {
    "took": 0,
    "timed_out": False,
    "hits": {"total": 0, "max_score": None, "hits": []},
    "aggregations": {},
}

@router.get("/samples")
def list_samples(
    size: int = Query(25, ge=1, le=1000, description="Max docs to return"),
):
    """Return a small page of sample docs.
    If ES is empty/unavailable, return an empty ES-shaped payload (never 500)."""
    index = "samples"  # change if your index name differs

    try:
        # Fast, simple query; adjust once your mapping is in place
        resp = es.search(
            index=index,
            query={"match_all": {}},
            size=size,
            track_total_hits=False,  # cheaper in ES 8/9
        )
        # Normalize the shape to what the FE expects
        took = resp.get("took", 0)
        timed_out = resp.get("timed_out", False)
        hits = resp.get("hits") or {}
        aggs = resp.get("aggregations") or {}
        total = hits.get("total", 0)
        if isinstance(total, dict):  # ES sometimes returns {"value": N, "relation": "eq"}
            total = int(total.get("value", 0))

        return {
            "took": took,
            "timed_out": timed_out,
            "hits": {
                "total": total,
                "max_score": hits.get("max_score"),
                "hits": hits.get("hits", []),
            },
            "aggregations": aggs,
        }

    except NotFoundError:
        # Index doesn't exist yet → treat as empty
        return ES_SHAPE_EMPTY
    except TransportError as e:
        # ES reachable but errored → degraded but JSON
        return ES_SHAPE_EMPTY
    except Exception as e:
        # Anything else (parsing, mapping, etc.) → still JSON
        return ES_SHAPE_EMPTY