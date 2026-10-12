"""Attach quality scores to the current trace and an optional source trace."""
import math
import re
from observability.instrumentation import annotate, outcome


def record_scores(scores, *, test_id, passed, source_trace_id=""):
    failed = bool(getattr(scores, "judge_failed", False))
    values = {key: getattr(scores, key, None) for key in ("relevance", "accuracy", "completeness", "helpfulness", "overall")}
    failed = failed or any(type(v) not in (int, float) or not math.isfinite(v) or not 0 <= v <= 1 for v in values.values())
    attrs = {"app.eval.test_id": test_id, "app.eval.judge_failed": failed, "app.eval.passed": bool(passed) and not failed}
    if not failed:
        attrs.update({"app.eval." + k: v for k, v in values.items()})
    if isinstance(source_trace_id, str) and re.fullmatch(r"[0-9a-f]{32}", source_trace_id):
        attrs["app.eval.source_trace_id"] = source_trace_id
    annotate(**attrs)
    if failed:
        outcome("degraded", code="JUDGE_FAILED")
