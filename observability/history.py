"""Bounded metadata-only SQLite backend, written by BatchSpanProcessor threads."""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from observability.policy import estimate_cost
from observability.sanitization import safe_attributes

_history = None


def get_history():
    return _history


class TraceHistory(SpanExporter):
    def __init__(self, policy):
        self.policy = policy
        self.path = Path(policy.history_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS traces (
                    trace_id TEXT PRIMARY KEY, request_id TEXT, name TEXT, priority INTEGER,
                    start_ns INTEGER, end_ns INTEGER, outcome TEXT, score REAL,
                    judge_failed INTEGER, test_id TEXT, passed INTEGER);
                CREATE INDEX IF NOT EXISTS trace_request ON traces(request_id);
                CREATE INDEX IF NOT EXISTS trace_start ON traces(start_ns);
                CREATE TABLE IF NOT EXISTS spans (
                    trace_id TEXT REFERENCES traces(trace_id) ON DELETE CASCADE,
                    span_id TEXT, start_ns INTEGER, payload TEXT,
                    PRIMARY KEY(trace_id, span_id));
                CREATE TABLE IF NOT EXISTS regressions (
                    trace_id TEXT PRIMARY KEY REFERENCES traces(trace_id) ON DELETE CASCADE,
                    turns TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS evaluations (
                    trace_id TEXT, evaluator_trace_id TEXT REFERENCES traces(trace_id) ON DELETE CASCADE,
                    span_id TEXT, payload TEXT, PRIMARY KEY(evaluator_trace_id, span_id));
                CREATE INDEX IF NOT EXISTS evaluation_target ON evaluations(trace_id);
                CREATE TABLE IF NOT EXISTS deleted_traces (trace_id TEXT PRIMARY KEY, deleted_ns INTEGER);
            """)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=1)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA secure_delete=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    def _prune(self, db):
        cutoff = time.time_ns() - self.policy.retention_days * 86400 * 1_000_000_000
        self._remove_traces(db, "SELECT trace_id FROM traces WHERE start_ns < ? OR trace_id IN (SELECT trace_id FROM traces ORDER BY start_ns DESC, trace_id LIMIT -1 OFFSET ?)",
                            (cutoff, self.policy.max_traces))
        db.execute("DELETE FROM deleted_traces WHERE deleted_ns < ?", (cutoff,))
        db.execute("DELETE FROM deleted_traces WHERE trace_id IN (SELECT trace_id FROM deleted_traces ORDER BY deleted_ns DESC LIMIT -1 OFFSET ?)", (self.policy.max_traces,))

    def _remove_traces(self, db, selection, args):
        targets = db.execute(f"SELECT DISTINCT trace_id FROM evaluations WHERE evaluator_trace_id IN ({selection})", args).fetchall()
        db.execute(f"DELETE FROM evaluations WHERE trace_id IN ({selection})", args)
        removed = db.execute(f"DELETE FROM traces WHERE trace_id IN ({selection})", args).rowcount
        for target in targets:
            self._refresh_score(db, target[0])
        return removed

    @staticmethod
    def _refresh_score(db, trace_id):
        row = db.execute("SELECT payload FROM evaluations WHERE trace_id=? ORDER BY rowid DESC LIMIT 1", (trace_id,)).fetchone()
        scored = json.loads(row[0])["scores"] if row else {}
        db.execute("UPDATE traces SET score=?, judge_failed=?, test_id=?, passed=? WHERE trace_id=?",
                   (scored.get("overall"), scored.get("judge_failed"), scored.get("test_id"), scored.get("passed"), trace_id))

    def export(self, spans):
        with self._db() as db:
            for span in spans:
                attrs = safe_attributes(span.attributes)
                # Internal compatibility state belongs to metrics, not historical detail.
                attrs = {k: v for k, v in attrs.items() if ".stats_" not in k and not k.endswith(".monitor_owner")}
                trace_id, span_id = f"{span.context.trace_id:032x}", f"{span.context.span_id:016x}"
                if db.execute("SELECT 1 FROM deleted_traces WHERE trace_id=?", (trace_id,)).fetchone():
                    continue
                priority = 3 if span.name == "evaluation.dialog" else 2 if span.name == "chat.pipeline" else 1 if span.parent is None else 0
                source_id = attrs.get("app.eval.source_trace_id")
                attached = source_id and source_id != trace_id
                db.execute("""INSERT INTO traces VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(trace_id) DO UPDATE SET
                    request_id=COALESCE(NULLIF(excluded.request_id,''),traces.request_id),
                    name=CASE WHEN excluded.priority>traces.priority THEN excluded.name ELSE traces.name END,
                    outcome=CASE WHEN excluded.priority>=traces.priority THEN excluded.outcome ELSE traces.outcome END,
                    priority=MAX(traces.priority,excluded.priority),
                    start_ns=MIN(traces.start_ns,excluded.start_ns), end_ns=MAX(traces.end_ns,excluded.end_ns),
                    score=COALESCE(excluded.score,traces.score),
                    judge_failed=COALESCE(excluded.judge_failed,traces.judge_failed),
                    test_id=COALESCE(excluded.test_id,traces.test_id),
                    passed=COALESCE(excluded.passed,traces.passed)""",
                    (trace_id, attrs.get("app.request_id", ""), span.name, priority, span.start_time, span.end_time,
                     attrs.get("app.outcome", "error" if span.status.status_code.name == "ERROR" else "success"),
                     None if attached else attrs.get("app.eval.overall"),
                     None if attached else attrs.get("app.eval.judge_failed"),
                     None if attached else attrs.get("app.eval.test_id"),
                     None if attached else attrs.get("app.eval.passed")))
                if "app.eval.judge_failed" in attrs:
                    target = source_id or trace_id
                    evaluation = {"evaluator_trace_id": trace_id, "span_id": span_id,
                                  "scores": {k.removeprefix("app.eval."): v for k, v in attrs.items() if k.startswith("app.eval.")}}
                    db.execute("INSERT OR REPLACE INTO evaluations VALUES(?,?,?,?)", (target, trace_id, span_id, json.dumps(evaluation)))
                    if attached:
                        db.execute("UPDATE traces SET score=?, judge_failed=?, test_id=?, passed=? WHERE trace_id=?",
                                   (attrs.get("app.eval.overall"), attrs.get("app.eval.judge_failed"), attrs.get("app.eval.test_id"), attrs.get("app.eval.passed"), target))
                # The source may arrive after its linked background evaluation.
                self._refresh_score(db, trace_id)
                payload = {"trace_id": trace_id, "span_id": span_id,
                           "parent_span_id": f"{span.parent.span_id:016x}" if span.parent else None,
                           "name": span.name, "start_ns": span.start_time, "end_ns": span.end_time,
                           "attributes": attrs,
                           "status": span.status.status_code.name,
                           "events": [{"name": e.name, "timestamp": e.timestamp, "attributes": safe_attributes(e.attributes)} for e in span.events],
                           "links": [{"trace_id": f"{link.context.trace_id:032x}", "span_id": f"{link.context.span_id:016x}"} for link in span.links]}
                cost = estimate_cost(attrs, self.policy.prices) if span.name == "llm.generate" else None
                if cost is not None:
                    payload["estimated_cost"] = dict(zip(("amount", "currency", "price_version"), cost))
                db.execute("INSERT OR REPLACE INTO spans VALUES(?,?,?,?)", (trace_id, span_id, span.start_time, json.dumps(payload)))
                db.execute("DELETE FROM spans WHERE trace_id=? AND span_id IN (SELECT span_id FROM spans WHERE trace_id=? ORDER BY start_ns,span_id LIMIT -1 OFFSET ?)",
                           (trace_id, trace_id, self.policy.max_spans_per_trace))
            self._prune(db)
        return SpanExportResult.SUCCESS

    def search(self, *, request_id=None, outcome=None, since_ns=None, limit=20, offset=0, low_score=False):
        terms, args = [], []
        for column, value in (("request_id", request_id), ("outcome", outcome)):
            if value is not None:
                terms.append(column + "=?")
                args.append(value)
        if since_ns is not None:
            terms.append("start_ns>=?")
            args.append(since_ns)
        if low_score:
            terms.append("passed=0 AND judge_failed=0")
        where = " WHERE " + " AND ".join(terms) if terms else ""
        with self._db() as db:
            self._prune(db)
            total = db.execute("SELECT COUNT(*) FROM traces" + where, args).fetchone()[0]
            rows = db.execute("SELECT *, (SELECT COUNT(*) FROM spans s WHERE s.trace_id=traces.trace_id) AS span_count, EXISTS(SELECT 1 FROM regressions r WHERE r.trace_id=traces.trace_id) AS replay_ready FROM traces" + where + " ORDER BY start_ns DESC, trace_id LIMIT ? OFFSET ?",
                              [*args, limit, offset]).fetchall()
            return {"total": total, "items": [dict(row) for row in rows]}

    def detail(self, trace_id):
        with self._db() as db:
            self._prune(db)
            row = db.execute("SELECT * FROM traces WHERE trace_id=?", (trace_id,)).fetchone()
            if row is None:
                return None
            spans = [json.loads(r[0]) for r in db.execute("SELECT payload FROM spans WHERE trace_id=? ORDER BY start_ns, span_id", (trace_id,))]
            evaluations = [json.loads(r[0]) for r in db.execute("SELECT payload FROM evaluations WHERE trace_id=?", (trace_id,))]
            return {**dict(row), "spans": spans, "evaluations": evaluations, "span_limit": self.policy.max_spans_per_trace,
                    "possibly_truncated": len(spans) >= self.policy.max_spans_per_trace}

    def save_regression(self, trace_id, turns):
        with self._db() as db:
            self._prune(db)
            if db.execute("SELECT 1 FROM traces WHERE trace_id=? AND passed=0 AND judge_failed=0", (trace_id,)).fetchone() is None:
                return False
            db.execute("INSERT OR REPLACE INTO regressions VALUES(?,?)", (trace_id, json.dumps(turns)))
            return True

    def regression(self, trace_id):
        with self._db() as db:
            self._prune(db)
            row = db.execute("SELECT turns FROM regressions WHERE trace_id=?", (trace_id,)).fetchone()
            return {"turns": json.loads(row[0]), "source_trace_id": trace_id} if row else None

    def delete(self, trace_id):
        with self._db() as db:
            db.execute("INSERT OR REPLACE INTO deleted_traces VALUES(?,?)", (trace_id, time.time_ns()))
            removed = self._remove_traces(db, "SELECT trace_id FROM traces WHERE trace_id=?", (trace_id,))
            self._prune(db)
            return removed > 0

    def shutdown(self):
        with self._db() as db:
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def force_flush(self, timeout_millis=30000):
        return True
