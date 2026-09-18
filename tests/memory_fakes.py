"""In-memory Redis/Chroma protocol fakes; no model, database, or native embeddings."""
import asyncio
import json
from collections import defaultdict
from types import SimpleNamespace

from memory.evolution import SourceMessage
from memory.evolution_runtime import MemoryEvolution, MemoryStore


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = defaultdict(dict)
        self.sets = defaultdict(set)
        self.lists = defaultdict(list)
        self.fail_ack = False

    async def incr(self, key):
        self.values[key] = int(self.values.get(key, 0)) + 1
        return self.values[key]

    async def hgetall(self, key):
        return dict(self.hashes[key])

    async def hsetnx(self, key, field, value):
        self.hashes[key].setdefault(field, value)

    async def hset(self, key, mapping):
        self.hashes[key].update(mapping)

    async def hdel(self, key, *fields):
        if self.fail_ack:
            raise ConnectionError("ack failure")
        for field in fields:
            self.hashes[key].pop(field, None)

    async def hlen(self, key):
        return len(self.hashes[key])

    async def sadd(self, key, item):
        self.sets[key].add(item)

    async def sscan_iter(self, key):
        for item in list(self.sets[key]):
            yield item

    async def lpush(self, key, value):
        self.lists[key].insert(0, value)

    async def lrange(self, key, start, end):
        return self.lists[key][start:] if end == -1 else self.lists[key][start:end + 1]

    async def ltrim(self, key, start, end):
        self.lists[key] = self.lists[key][start:end + 1]

    async def llen(self, key):
        return len(self.lists[key])

    async def expire(self, key, ttl):
        return True

    async def get(self, key):
        return self.values.get(key)

    async def setex(self, key, ttl, value):
        self.values[key] = value

    async def aclose(self):
        pass

    def pipeline(self, transaction=True):
        owner = self
        class Pipeline:
            def __init__(self):
                self.commands = []
            def __getattr__(self, name):
                def queue(*args, **kwargs):
                    self.commands.append((name, args, kwargs))
                    return self
                return queue
            async def execute(self):
                # Fake commands do not suspend, so a batch is indivisible in this event loop.
                return [await getattr(owner, name)(*args, **kwargs) for name, args, kwargs in self.commands]
        return Pipeline()


class FakeCollection:
    def __init__(self):
        self.documents = {}
        self.writes = []
        self.fail_write = False
    def get(self, ids=None, where=None):
        return {"documents": [self.documents[key] for key in ids or [] if key in self.documents]}
    def upsert(self, ids, documents, **kwargs):
        if self.fail_write:
            raise ConnectionError("write failure")
        self.documents.update(zip(ids, documents))
        self.writes.append((list(ids), list(documents), kwargs))


def source(seq, text="以后退款优先支付宝", user="u", conv="c", project="default"):
    return SourceMessage(f"{user}-{seq}", user, conv, seq,
                         f"2026-09-16T12:00:{seq % 60:02d}+00:00", text, project_id=project)


def raw_candidate(src, key="refund_method", value="alipay", operation="set", **changes):
    raw = {"type": "background" if key in {"technical_environment", "common_products"} else "preference",
           "key": key, "value": value, "operation": operation, "confidence": .95,
           "source_message_id": src.message_id, "quote": src.content}
    raw.update(changes)
    return raw


class FakeModel:
    def __init__(self, factory=None):
        self.factory = factory or (lambda src: raw_candidate(src))
        self.messages = self
        self.calls = []
        self.error = None
        self.raw_response = None
        self.entered = None
        self.release = None
    async def create(self, **kwargs):
        self.calls.append(kwargs)
        if self.entered:
            self.entered.set()
        if self.release:
            await self.release.wait()
        if self.error:
            raise self.error
        payload = json.loads(kwargs["messages"][0]["content"].splitlines()[-1])
        candidates = [self.factory(SourceMessage(**s)) for s in payload["messages"]]
        text = self.raw_response if self.raw_response is not None else json.dumps({"candidates": [c for c in candidates if c is not None]})
        return SimpleNamespace(content=[{"type": "text", "text": text}])
    async def close(self):
        pass


def engine(factory=None):
    redis, collection, model = FakeRedis(), FakeCollection(), FakeModel(factory)
    evolution = MemoryEvolution(redis, MemoryStore(collection), model, "test-model")
    return evolution, redis, collection, model


async def enqueue(evolution, src, schedule=True):
    pipe = evolution.redis.pipeline()
    evolution.enqueue_commands(pipe, src)
    await pipe.execute()
    if schedule:
        evolution.schedule(src.project_id, src.user_id)


async def idle(evolution):
    for _ in range(100):
        tasks = list(evolution._tasks.values())
        if not tasks:
            return
        await asyncio.gather(*tasks)
        await asyncio.sleep(0)
    raise AssertionError("worker did not become idle")
