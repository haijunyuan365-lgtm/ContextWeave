"""Typed, evidence-backed candidates and a deterministic memory resolver."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


# Explicit registry: model output cannot invent new categories of personal data.
FIELDS = {
    "refund_method": ("preference", False, r"退款|退回|refund"),
    "payment_method": ("preference", False, r"支付(?!宝)|付款|payment"),
    "response_language": ("preference", False, r"回复|回答|交流|语言|中文|英文|英语|日语|language"),
    "response_style": ("preference", False, r"回复|回答|解释|步骤|风格|reply|response"),
    "contact_channel": ("preference", False, r"联系|通知|contact"),
    "common_products": ("background", True, r"常用|长期|一直|平时|通常|日常|主要|usually"),
    "technical_environment": ("background", True, r"常用|长期|一直|平时|通常|日常|主要|usually"),
}
ALIASES = {
    "refund_method": {"alipay": ["支付宝", "alipay"], "wechat": ["微信", "wechat"],
                      "bank_card": ["银行卡", "bank card"], "original_route": ["原路", "original route"]},
    "payment_method": {"alipay": ["支付宝", "alipay"], "wechat": ["微信", "wechat"],
                       "bank_card": ["银行卡", "bank card"]},
    "response_language": {"zh": ["中文", "汉语", "chinese"], "en": ["英文", "英语", "english"],
                          "ja": ["日语", "japanese"]},
    "response_style": {"concise": ["简短", "简洁", "精简", "concise"],
                       "detailed": ["详细", "详细点", "detailed"], "step_by_step": ["分步骤", "逐步", "step by step"]},
    "contact_channel": {"email": ["邮件", "邮箱", "email"], "phone": ["电话", "phone"],
                        "sms": ["短信", "sms"], "in_app": ["站内", "应用内", "in-app"]},
}
EXPLICIT = re.compile(
    r"以后|今后|优先|偏好|习惯|喜欢|默认|请记住|记一下|改为|改成|不再|撤回|忘记|不要.*记|别再.*记|"
    r"请.*(?:中文|英文|英语|日语|简短|简洁|详细|分步骤|邮件|电话)|(?:用|通过).*(?:回复|回答|联系我)|prefer|remember", re.I)
UNSAFE = re.compile(r"密码|验证码|身份证|银行卡号|余额|物流状态|订单状态|退款成功|到账|已退款|已支付|已发货|password|verification code|balance", re.I)
AMBIGUOUS = re.compile(r"如果|假如|假设|比如|例如|我朋友|我同事|我家人|他说|她说|客户说|用户说|假装|if |suppose|my friend", re.I)
NEGATIVE = re.compile(r"不要|不想|不喜欢|不再|别用|不用|不能|不希望|别再|don't|do not", re.I)
RETRACT = re.compile(r"撤回|忘记|不再|取消.*偏好|不要.*(?:记住|优先)|别再.*(?:记|优先)|forget|no longer", re.I)


@dataclass(frozen=True)
class SourceMessage:
    message_id: str
    user_id: str
    conv_id: str
    sequence: int
    timestamp: str
    content: str
    role: str = "user"
    project_id: str = "default"


@dataclass(frozen=True)
class MemoryCandidate:
    type: str
    key: str
    value: Any
    operation: str
    confidence: float
    source: SourceMessage
    quote: str

    def evidence(self) -> dict:
        return {"message_id": self.source.message_id, "conv_id": self.source.conv_id,
                "sequence": self.source.sequence, "timestamp": self.source.timestamp, "quote": self.quote}


@dataclass
class MemoryRecord:
    memory_id: str
    project_id: str
    user_id: str
    type: str
    key: str
    value: Any
    created_at: str
    updated_at: str
    confidence: float
    source: dict
    version: int = 1
    status: str = "active"
    member_sources: dict[str, dict] = field(default_factory=dict)


@dataclass
class MemoryDecision:
    action: str
    key: str
    reason: str
    source_message_id: str
    previous_value: Any = None
    value: Any = None
    version: int = 0
    timestamp: str = field(default_factory=now)
    source: dict = field(default_factory=dict)
    previous_source: dict = field(default_factory=dict)


def empty_snapshot(user_id: str, project_id: str = "default") -> dict:
    return {"schema_version": 2, "project_id": project_id, "user_id": user_id, "version": 0, "records": {},
            "watermarks": {}, "events": [], "processed_source_ids": [], "last_success_at": None}


def canonical_value(key: str, value: Any) -> Any:
    if FIELDS[key][1]:
        if not isinstance(value, list) or not value or len(value) > 10:
            raise ValueError("invalid collection value")
        if any(not isinstance(v, str) or not v.strip() or len(v) > 80 or UNSAFE.search(v) for v in value):
            raise ValueError("invalid member")
        normalized = {}
        for item in value:
            normalized.setdefault(item.strip().casefold(), item.strip())
        return list(normalized.values())
    if not isinstance(value, str):
        raise ValueError("invalid scalar value")
    for canonical, aliases in ALIASES[key].items():
        if value.lower().strip() in [canonical, *aliases]:
            return canonical
    raise ValueError("unregistered value")


def validate_candidate(raw: Any, sources: dict[str, SourceMessage], threshold: float,
                       records: dict | None = None) -> tuple[MemoryCandidate | None, str]:
    required = {"type", "key", "value", "operation", "confidence", "source_message_id", "quote"}
    if not isinstance(raw, dict) or set(raw) != required:
        return None, "invalid_fields"
    key = raw["key"]
    if not isinstance(key, str) or key not in FIELDS or raw["type"] != FIELDS[key][0]:
        return None, "unregistered_field"
    source_id = raw["source_message_id"]
    source = sources.get(source_id) if isinstance(source_id, str) else None
    if source is None or source.role != "user":
        return None, "invalid_source"
    quote = raw["quote"]
    if not isinstance(quote, str) or not quote.strip() or len(quote) > 200 or quote not in source.content:
        return None, "invalid_evidence"
    score = raw["confidence"]
    if type(score) not in (int, float) or not math.isfinite(score) or not threshold <= score <= 1:
        return None, "low_confidence"
    if UNSAFE.search(quote) or AMBIGUOUS.search(source.content):
        return None, "unsafe_or_ambiguous"
    if not re.search(FIELDS[key][2], quote, re.I):
        possible = {k for k, record in (records or {}).items() if record["status"] == "active"
                    and k in ALIASES and isinstance(raw["value"], str)
                    and any(raw["value"].lower() in [v, *aliases] for v, aliases in ALIASES[k].items())}
        # Earlier *user* assertions in this extraction batch can disambiguate a change,
        # without trusting another model-generated candidate as context.
        for earlier in sources.values():
            if (earlier.sequence >= source.sequence or earlier.role != "user" or earlier.user_id != source.user_id
                    or earlier.project_id != source.project_id
                    or not EXPLICIT.search(earlier.content) or UNSAFE.search(earlier.content)
                    or AMBIGUOUS.search(earlier.content) or NEGATIVE.search(earlier.content)):
                continue
            for field_name, aliases in ALIASES.items():
                if (re.search(FIELDS[field_name][2], earlier.content, re.I) and isinstance(raw["value"], str)
                        and any(raw["value"].lower() in [v, *names] for v, names in aliases.items())):
                    possible.add(field_name)
        if possible != {key} or not re.search(r"改为|改成|change to", quote, re.I):
            return None, "missing_field_evidence"
    operation = raw["operation"]
    if not isinstance(operation, str) or operation not in {"set", "retract"}:
        return None, "invalid_operation"
    if operation == "retract":
        if not RETRACT.search(quote):
            return None, "missing_retraction"
        if FIELDS[key][1]:
            # V1 retracts a whole field; member-by-member removal is not inferred.
            return None, "unsupported_collection_retraction"
    elif NEGATIVE.search(quote) or not re.search(r"我|以后|今后|优先|请|长期|平时|通常|日常|一直|i |my |prefer", source.content, re.I):
        return None, "not_personal_assertion"
    elif not FIELDS[key][1] and not (EXPLICIT.search(quote) or re.search(r"喜欢|希望|默认|用.*联系我|请.*(?:用|通过|回复|回答|联系)", quote)):
        return None, "not_explicit_preference"
    try:
        value = None if operation == "retract" and raw["value"] is None else canonical_value(key, raw["value"])
    except ValueError:
        return None, "invalid_value"
    if value is not None:
        aliases = value if isinstance(value, list) else ALIASES[key][value]
        if isinstance(value, list):
            supported = all(v.casefold() in quote.casefold() for v in aliases)
        else:
            supported = any(v.casefold() in quote.casefold() for v in aliases)
        if not supported:
            return None, "unsupported_value"
    elif not re.search(r"撤回|忘记|偏好|记住|forget", quote, re.I):
        return None, "ambiguous_retraction"
    return MemoryCandidate(raw["type"], key, value, operation, float(score), source, quote), "valid"


class MemoryResolver:
    def resolve(self, snapshot: dict, candidates: list[MemoryCandidate]) -> tuple[dict, list[MemoryDecision]]:
        result = copy.deepcopy(snapshot)
        decisions = []
        seen = set(snapshot["processed_source_ids"])
        groups: dict[tuple, list[MemoryCandidate]] = {}
        for candidate in candidates:
            groups.setdefault((candidate.source.sequence, candidate.key, candidate.source.message_id), []).append(candidate)
        for _, group in sorted(groups.items()):
            c = group[0]
            key, sid = c.key, c.source.message_id
            old = result["records"].get(key)
            prior = copy.deepcopy(old["value"]) if old else None
            reason = ""
            if (c.source.user_id != snapshot["user_id"] or c.source.project_id != snapshot["project_id"]
                    or c.source.role != "user"):
                reason = "invalid_source"
            elif sid in seen:
                reason = "duplicate_source"
            elif len({json.dumps([v.operation, v.value], sort_keys=True) for v in group}) > 1:
                reason = "conflicting_candidates"
            elif c.source.sequence <= result["watermarks"].get(key, 0):
                reason = "stale_source"
            elif c.operation == "retract" and c.value is not None and (not old or old["value"] != c.value):
                reason = "retraction_value_mismatch"
            if reason:
                decisions.append(MemoryDecision("SKIP", key, reason, sid, version=old["version"] if old else 0))
                continue
            # A newer reaffirmation is still a watermark: a late old conflict cannot win.
            result["watermarks"][key] = c.source.sequence
            value = copy.deepcopy(c.value)
            status = "inactive" if c.operation == "retract" else "active"
            if old and old["status"] == status:
                if status == "inactive" or old["value"] == value:
                    decisions.append(MemoryDecision("SKIP", key, "duplicate_value", sid, version=old["version"]))
                    continue
                if FIELDS[key][1]:
                    combined = {v.casefold(): v for v in old["value"]}
                    for member in value:
                        combined.setdefault(member.casefold(), member)
                    value = list(combined.values())
                    if value == old["value"]:
                        decisions.append(MemoryDecision("SKIP", key, "duplicate_value", sid, version=old["version"]))
                        continue
            stamp = now()
            record = MemoryRecord(
                memory_id=hashlib.sha256(
                    f"{snapshot['project_id']}:{snapshot['user_id']}:{c.type}:{key}".encode()).hexdigest(),
                project_id=snapshot["project_id"], user_id=snapshot["user_id"],
                type=c.type, key=key, value=prior if status == "inactive" else value,
                created_at=old["created_at"] if old else stamp, updated_at=stamp,
                confidence=c.confidence, source=c.evidence(), version=old["version"] + 1 if old else 1, status=status,
                member_sources=copy.deepcopy(old.get("member_sources", {})) if old else {},
            )
            if FIELDS[key][1]:
                for member in c.value:
                    canonical_member = next(v for v in value if v.casefold() == member.casefold())
                    record.member_sources.setdefault(canonical_member, c.evidence())
            action = "UPDATE" if c.operation == "retract" or old else "CREATE"
            if old and FIELDS[key][1] and status == "active" and old["status"] == "active":
                action = "MERGE"
            result["records"][key] = asdict(record)
            decisions.append(MemoryDecision(action, key, "explicit_retraction" if status == "inactive" else "supported_user_statement",
                                            sid, prior, record.value, record.version, source=c.evidence(),
                                            previous_source=copy.deepcopy(old["source"]) if old else {}))
        return result, decisions


def legacy_fields(profile: dict) -> dict:
    """Only deterministic mappings; opaque old text is inspectable but not active evidence."""
    mapped = {}
    for key in FIELDS:
        if key in profile:
            try:
                mapped[key] = canonical_value(key, profile[key])
            except ValueError:
                pass
    for text in profile.get("preferences", []) if isinstance(profile.get("preferences"), list) else []:
        if not isinstance(text, str) or UNSAFE.search(text) or AMBIGUOUS.search(text) or NEGATIVE.search(text):
            continue
        for key, (_, multiple, cue) in FIELDS.items():
            if multiple or not re.search(cue, text, re.I):
                continue
            matches = [v for v, aliases in ALIASES[key].items() if any(a in text.lower() for a in aliases)]
            if len(matches) == 1:
                mapped.setdefault(key, matches[0])
    return mapped


def effective_profile(snapshot: dict, legacy: dict) -> dict:
    values = legacy_fields(legacy)
    provenance = {key: {"kind": "legacy", "source_message_id": None} for key in values}
    for key, record in snapshot["records"].items():
        values.pop(key, None)
        provenance.pop(key, None)
        if record["status"] == "active":
            values[key] = copy.deepcopy(record["value"])
            provenance[key] = {"kind": "user_message", "source_message_id": record["source"]["message_id"],
                               "version": record["version"]}
    if not values:
        return {}
    return {"preferences": {k: v for k, v in values.items() if FIELDS[k][0] == "preference"},
            "entities": {k: v for k, v in values.items() if FIELDS[k][0] == "background"}, "sources": provenance}
