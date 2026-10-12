"""Only structured metadata is exported; never exception text or stack traces."""
import asyncio
import hashlib
from urllib.parse import urlsplit, urlunsplit

MAX_STRING_LENGTH = 256
_PRIVATE_KEYS = ("prompt", "message", "content", "query", "password", "secret", "api_key",
                 "authorization", "cookie", "tool.arguments", "tool.output", "user_id", "conv_id",
                 "exception.stacktrace")


def hash_identifier(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def error_code(exc: BaseException) -> str:
    if isinstance(exc, asyncio.CancelledError):
        return "CANCELLED"
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return "TIMEOUT"
    return "EXECUTION_ERROR"


def safe_attributes(attributes) -> dict:
    result = {}
    for key, value in (attributes or {}).items():
        if key not in {"app.rag.query_index", "app.rag.subquery_count"} and any(part in key.lower() for part in _PRIVATE_KEYS):
            continue
        if value is None:
            continue
        if isinstance(value, str):
            if key in {"http.url", "url.full"}:
                parsed = urlsplit(value)
                # Route templates avoid exporting IDs embedded in other API paths.
                path = attributes.get("http.route", "/")
                value = urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
            result[key] = value[:MAX_STRING_LENGTH]
        elif isinstance(value, (bool, int, float)):
            result[key] = value
        elif isinstance(value, (list, tuple)):
            if all(isinstance(item, str) for item in value):
                result[key] = tuple(item[:MAX_STRING_LENGTH] for item in value[:16])
    return result
