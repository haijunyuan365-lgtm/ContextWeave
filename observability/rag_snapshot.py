"""Small rank snapshots contain hashes and numeric scores, never retrieved text."""
from functools import wraps
import inspect
import json
import math

from opentelemetry import trace
from observability.sanitization import hash_identifier


def candidates(items, direction="output", *, source=""):
    if not isinstance(items, list):
        return items
    refs = []
    for rank, item in enumerate(items[:16], 1):
        if not isinstance(item, dict) or not item.get("chunk_id"):
            continue
        score = item.get("score")
        score = score if type(score) in (int, float) and math.isfinite(score) else None
        try:
            refs.append(json.dumps([hash_identifier(str(item["chunk_id"])), rank, score], separators=(",", ":")))
        except (UnicodeError, TypeError, ValueError):
            # A malformed reference must not change retrieval results.
            continue
    trace.get_current_span().add_event("rag.candidates", {
        "app.rag.direction": direction, "app.rag.source": source,
        "app.rag.candidate_refs": refs, "app.rag.candidate_count": len(items),
        "app.rag.snapshot_truncated": len(items) > 16,
    })
    return items


def ranked(function):
    """Place inside @instrumented so early returns and fallbacks are recorded too."""
    signature = inspect.signature(function)
    @wraps(function)
    async def wrapped(*args, **kwargs):
        items = signature.bind(*args, **kwargs).arguments.get("items", [])
        candidates(items, "input")
        return candidates(await function(*args, **kwargs))
    return wrapped
