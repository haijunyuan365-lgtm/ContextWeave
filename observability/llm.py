"""Anthropic-protocol call metadata, using GenAI fields pinned to semconv 1.37.0."""
from collections.abc import Mapping

from opentelemetry.trace import SpanKind

from observability.instrumentation import observed


def _field(value, key):
    return value.get(key) if isinstance(value, Mapping) else getattr(value, key, None)


async def generate(client, *, stage: str, round_number: int | None = None, **kwargs):
    # provider.name identifies the telemetry protocol flavour, even for compatible endpoints.
    attributes = {"gen_ai.provider.name": "anthropic", "gen_ai.operation.name": "chat",
                  "gen_ai.request.model": kwargs.get("model"), "app.llm.stage": stage,
                  "app.llm.round": round_number, "app.gen_ai.semconv_version": "1.37.0"}
    with observed("llm.generate", span_type="llm", kind=SpanKind.CLIENT, **attributes) as span:
        response = await client.messages.create(**kwargs)
        model = _field(response, "model")
        if isinstance(model, str):
            span.set_attribute("gen_ai.response.model", model)
        stop = _field(response, "stop_reason")
        if isinstance(stop, str):
            span.set_attribute("gen_ai.response.finish_reasons", [stop])
        usage = _field(response, "usage")
        fields = {"input_tokens": "gen_ai.usage.input_tokens", "output_tokens": "gen_ai.usage.output_tokens",
                  "cache_read_input_tokens": "app.llm.cache_read_tokens",
                  "cache_creation_input_tokens": "app.llm.cache_write_tokens"}
        for source, target in fields.items():
            value = _field(usage, source)
            if type(value) is int and value >= 0:
                span.set_attribute(target, value)
        return response
