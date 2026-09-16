import asyncio
import json
from dataclasses import replace

import pytest

from memory.evolution import MemoryResolver, effective_profile, empty_snapshot, validate_candidate
from memory.evolution_runtime import CandidateExtractor, MemoryEvolution, MemoryStore
from memory_fakes import engine, enqueue, idle, raw_candidate, source, FakeModel


def candidate(src, key="refund_method", value="alipay", operation="set", records=None):
    result, reason = validate_candidate(raw_candidate(src, key, value, operation), {src.message_id: src}, .8, records)
    assert result, reason
    return result


def test_create_update_retract_and_reactivate_preserve_history_and_identity():
    resolver = MemoryResolver()
    snap, first = resolver.resolve(empty_snapshot("u"), [candidate(source(1))])
    created = snap["records"]["refund_method"]
    assert first[0].action == "CREATE" and created["source"]["message_id"] == "u-1"
    snap, second = resolver.resolve(snap, [candidate(source(2, "以后改为微信"), value="wechat", records=snap["records"])])
    record = snap["records"]["refund_method"]
    assert second[0].action == "UPDATE" and second[0].previous_value == "alipay"
    assert record["memory_id"] == created["memory_id"] and record["version"] == 2
    snap, decisions = resolver.resolve(snap, [candidate(source(3, "请忘记我的退款方式偏好"), value=None, operation="retract")])
    assert decisions[0].action == "UPDATE" and snap["records"]["refund_method"]["status"] == "inactive"
    assert effective_profile(snap, {"preferences": ["退款优先支付宝"]}) == {}
    snap, decisions = resolver.resolve(snap, [candidate(source(4))])
    assert decisions[0].action == "UPDATE" and snap["records"]["refund_method"]["status"] == "active"


def test_merge_keeps_member_provenance_and_duplicate_value_skips():
    resolver = MemoryResolver()
    a = candidate(source(1, "我长期使用 Windows"), "technical_environment", ["Windows"])
    b = candidate(source(2, "我长期使用 Linux 和 Windows"), "technical_environment", ["Linux", "Windows"])
    snap, _ = resolver.resolve(empty_snapshot("u"), [a])
    snap, decisions = resolver.resolve(snap, [b])
    assert decisions[0].action == "MERGE"
    record = snap["records"]["technical_environment"]
    assert record["value"] == ["Windows", "Linux"]
    assert record["member_sources"]["Windows"]["message_id"] == "u-1"
    assert record["member_sources"]["Linux"]["message_id"] == "u-2"
    snap, decisions = resolver.resolve(snap, [replace(b, source=source(3, b.source.content))])
    assert decisions[0].action == "SKIP" and record["version"] == 2


def test_late_old_conflict_cannot_override_newer_reaffirmation():
    resolver = MemoryResolver()
    snap, _ = resolver.resolve(empty_snapshot("u"), [candidate(source(1))])
    snap, decisions = resolver.resolve(snap, [candidate(source(3))])
    assert decisions[0].reason == "duplicate_value"
    snap, decisions = resolver.resolve(snap, [candidate(source(2, "以后退款优先微信"), value="wechat")])
    assert decisions[0].reason == "stale_source"
    assert snap["records"]["refund_method"]["value"] == "alipay"


@pytest.mark.parametrize("text,changes", [
    ("如果以后退款优先支付宝", {}),
    ("我朋友以后退款优先支付宝", {}),
    ("以后退款不要支付宝", {}),
    ("我的退款成功并通过支付宝到账", {}),
    ("我的退款退到了支付宝", {}),
    ("以后退款优先支付宝，密码1234", {}),
    ("以后退款优先支付宝", {"key": "order_status"}),
    ("以后退款优先支付宝", {"quote": "我喜欢微信"}),
    ("以后退款优先支付宝", {"source_message_id": "other"}),
    ("以后退款优先支付宝", {"confidence": .5}),
    ("以后退款优先支付宝", {"confidence": True}),
    ("以后退款优先支付宝", {"confidence": float("nan")}),
    ("以后退款优先支付宝", {"value": "wechat"}),
    ("以后退款优先支付宝", {"operation": []}),
])
def test_invalid_or_unsafe_candidates_skip(text, changes):
    src = source(1, text)
    value, _ = validate_candidate(raw_candidate(src, **changes), {src.message_id: src}, .8)
    assert value is None


def test_assistant_is_not_a_source_and_ambiguous_modification_is_not_guessed():
    src = replace(source(1), role="assistant")
    assert validate_candidate(raw_candidate(src), {src.message_id: src}, .8)[0] is None
    src = source(2, "以后改为微信")
    records = {"refund_method": {"status": "active"}, "payment_method": {"status": "active"}}
    assert validate_candidate(raw_candidate(src, value="wechat"), {src.message_id: src}, .8, records)[0] is None


def test_legacy_mapping_does_not_activate_opaque_text_and_new_values_win():
    legacy = {"preferences": ["退款优先微信", "神秘不可识别内容"], "response_language": "中文"}
    profile = effective_profile(empty_snapshot("u"), legacy)
    assert profile["preferences"] == {"refund_method": "wechat", "response_language": "zh"}
    assert profile["sources"]["refund_method"] == {"kind": "legacy", "source_message_id": None}
    snap, _ = MemoryResolver().resolve(empty_snapshot("u"), [candidate(source(1))])
    assert effective_profile(snap, legacy)["preferences"]["refund_method"] == "alipay"
    assert "神秘" not in json.dumps(effective_profile(snap, legacy), ensure_ascii=False)


def test_threshold_explicit_trigger_and_no_duplicate_extraction():
    async def scenario():
        evo, redis, collection, model = engine(lambda src: None)
        for n in range(1, 5):
            await enqueue(evo, source(n, "普通咨询"))
            await idle(evo)
        assert not model.calls
        await enqueue(evo, source(5, "普通咨询"))
        await idle(evo)
        assert len(model.calls) == 1 and (await evo.status("u"))["pending_count"] == 0
        await enqueue(evo, source(6))
        await idle(evo)
        assert len(model.calls) == 2
        await enqueue(evo, source(6))
        await idle(evo)
        assert len(model.calls) == 2 and len(collection.writes) == 2
    asyncio.run(scenario())


def test_max_twenty_messages_per_batch():
    async def scenario():
        evo, _, _, model = engine(lambda src: None)
        for n in range(1, 26):
            await enqueue(evo, source(n, "普通咨询"), schedule=False)
        evo.schedule("u")
        await idle(evo)
        sizes = [len(json.loads(c["messages"][0]["content"].splitlines()[-1])["messages"]) for c in model.calls]
        assert sizes == [20, 5]
    asyncio.run(scenario())


def test_out_of_order_arrival_and_independent_users():
    async def scenario():
        evo, _, _, _ = engine(lambda src: raw_candidate(src, value="wechat" if "微信" in src.content else "alipay"))
        await enqueue(evo, source(2, "以后退款优先微信"))
        await idle(evo)
        await enqueue(evo, source(1))
        await enqueue(evo, source(1, user="other"))
        await idle(evo)
        assert (await evo.profile("u", {}))["preferences"]["refund_method"] == "wechat"
        assert (await evo.profile("other", {}))["preferences"]["refund_method"] == "alipay"
        assert (await evo.events("u"))[0]["reason"] == "stale_source"
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["write", "ack", "model", "json", "schema", "timeout"])
def test_failures_preserve_pending_and_recovery_is_idempotent(failure):
    async def scenario():
        evo, redis, collection, model = engine()
        if failure == "write":
            collection.fail_write = True
        elif failure == "ack":
            redis.fail_ack = True
        elif failure == "model":
            model.error = RuntimeError("private error text")
        elif failure == "json":
            model.raw_response = "not json"
        elif failure == "schema":
            model.raw_response = '{"candidates": [{"key": "refund_method"}]}'
        else:
            model.release = asyncio.Event()
            evo.extractor.timeout = .01
        await enqueue(evo, source(1))
        await idle(evo)
        assert len(await evo.pending("u")) == 1
        assert (await evo.status("u"))["last_error_category"]
        assert len(collection.writes) == (1 if failure == "ack" else 0)
        collection.fail_write = redis.fail_ack = False
        model.error = model.raw_response = model.release = None
        recovered = MemoryEvolution(redis, MemoryStore(collection), model, "test")
        await recovered.start()
        await idle(recovered)
        snapshot = await recovered.store.read("u")
        assert snapshot["version"] == 1 and len(snapshot["events"]) == 1
        assert snapshot["records"]["refund_method"]["value"] == "alipay"
        assert not await recovered.pending("u")
        assert not (await recovered.status("u"))["last_error_category"]
        assert len(model.calls) == (1 if failure == "ack" else 2)
    asyncio.run(scenario())


def test_same_user_worker_coalesces_and_shutdown_retains_pending():
    async def scenario():
        evo, _, _, model = engine()
        model.entered, model.release = asyncio.Event(), asyncio.Event()
        await enqueue(evo, source(1))
        await model.entered.wait()
        for _ in range(5):
            evo.schedule("u")
        assert len(evo._tasks) == 1 and len(model.calls) == 1
        await enqueue(evo, source(2))
        evo.SHUTDOWN_TIMEOUT = .01
        await evo.close()
        assert len(await evo.pending("u")) == 2 and not evo._tasks
        assert (await evo.store.read("u"))["version"] == 0
    asyncio.run(scenario())


def test_disabled_evolution_retains_existing_memory_and_pending(monkeypatch):
    async def scenario():
        evo, redis, collection, model = engine()
        await enqueue(evo, source(1))
        await idle(evo)
        monkeypatch.setenv("ECHOMIND_MEMORY_EVOLUTION_ENABLED", "false")
        disabled = MemoryEvolution(redis, MemoryStore(collection), model, "test")
        await enqueue(disabled, source(2))
        assert not disabled._tasks
        assert (await disabled.profile("u", {}))["preferences"]["refund_method"] == "alipay"
        assert len(await disabled.pending("u")) == 1
    asyncio.run(scenario())


def test_logs_do_not_include_values_or_source_text(caplog):
    async def scenario():
        evo, _, _, _ = engine()
        with caplog.at_level("INFO", logger="memory.evolution_runtime"):
            await enqueue(evo, source(1))
            await idle(evo)
        assert "CREATE" in caplog.text
        assert "支付宝" not in caplog.text and "alipay" not in caplog.text
    asyncio.run(scenario())


def test_batch_change_uses_earlier_user_evidence_and_sorts_model_output():
    first, second = source(1), source(2, "以后改为微信")
    sources = {s.message_id: s for s in [first, second]}
    later, _ = validate_candidate(raw_candidate(second, value="wechat"), sources, .8)
    assert later is not None
    snap, decisions = MemoryResolver().resolve(empty_snapshot("u"), [later, candidate(first)])
    assert [d.action for d in decisions] == ["CREATE", "UPDATE"]
    assert snap["records"]["refund_method"]["value"] == "wechat"
    assert decisions[1].previous_source["message_id"] == first.message_id


def test_case_variants_of_stable_environment_are_not_new_members():
    resolver = MemoryResolver()
    snap, _ = resolver.resolve(empty_snapshot("u"), [candidate(source(1, "我长期使用 Windows"), "technical_environment", ["Windows"])])
    snap, decisions = resolver.resolve(snap, [candidate(source(2, "我长期使用 windows 和 Linux"), "technical_environment", ["windows", "Linux"])])
    assert snap["records"]["technical_environment"]["value"] == ["Windows", "Linux"]
    assert set(snap["records"]["technical_environment"]["member_sources"]) == {"Windows", "Linux"}
    assert decisions[0].action == "MERGE"


def test_explicit_language_preference_does_not_wait_for_five_turns():
    async def scenario():
        evo, _, _, model = engine(lambda src: raw_candidate(src, "response_language", "zh"))
        await enqueue(evo, source(1, "请用中文回复"))
        await idle(evo)
        assert len(model.calls) == 1
        assert (await evo.profile("u", {}))["preferences"]["response_language"] == "zh"
    asyncio.run(scenario())
