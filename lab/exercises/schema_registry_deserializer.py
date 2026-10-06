#!/usr/bin/env python3
"""
Exercise 5 (hard) — resolve schemas from Schema Registry instead of local files.

The lab's consumer reads the alert schema off disk:

    def load_velocity_alert_schema() -> str:
        schema_path = os.path.normpath(os.path.join(
            os.path.dirname(__file__),
            "../fashion-inventory-setup/schemas/velocity-anomaly-alert.schema.json"))
        with open(schema_path, encoding="utf-8") as f:
            return f.read()

    def build_value_deserializer() -> JSONDeserializer:
        return JSONDeserializer(load_velocity_alert_schema(), ...)

That works, but the file on your laptop and the schema registered against the
topic can drift apart silently. Resolving from the registry makes the registry
the single source of truth — if someone evolves the schema, the consumer picks
it up without a redeploy.

Drop this file next to consume_velocity_alerts_with_agent.py and swap the
deserializer construction for:

    from schema_registry_deserializer import build_registry_deserializer
    value_deserializer = build_registry_deserializer(
        _schema_registry_client, "fashion.velocity.anomalies")

Things to watch for, which is what makes this the hard exercise:

1. Subject naming. The default TopicNameStrategy means the subject is
   "<topic>-value", not the topic name. Get this wrong and you get a confusing
   404 from the registry rather than a useful error.
2. Startup coupling. The consumer now fails to start if the registry is
   unreachable, where before it would start and fail later. That is usually
   the behaviour you want, but it is a real change.
3. Latency and caching. SchemaRegistryClient caches by schema id, so the
   lookup cost is paid once, not per message.
"""

from __future__ import annotations

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


def fetch_latest_schema(client: SchemaRegistryClient, topic: str) -> str:
    """Return the latest registered value schema for a topic, as a JSON string.

    Subject follows TopicNameStrategy: "<topic>-value".
    """
    subject = f"{topic}-value"
    try:
        registered = client.get_latest_version(subject)
    except Exception as err:  # noqa: BLE001 — surfaced with context below
        raise RuntimeError(
            f"Could not fetch schema for subject '{subject}'. "
            f"Confirm the schema is registered (Section 2.4) and that "
            f"SCHEMA_REGISTRY_* credentials are correct. Underlying error: {err}"
        ) from err

    return registered.schema.schema_str


def build_registry_deserializer(
    client: SchemaRegistryClient,
    topic: str,
) -> JSONDeserializer:
    """JSONDeserializer whose schema comes from the registry, not from disk."""
    return JSONDeserializer(
        fetch_latest_schema(client, topic),
        from_dict=lambda obj, ctx: obj,
    )


if __name__ == "__main__":
    # Standalone check: prove the registry resolves before touching the consumer.
    import os
    from dotenv import load_dotenv

    load_dotenv()
    sr = SchemaRegistryClient({
        "url": os.environ["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": (
            f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:"
            f"{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
        ),
    })
    for t in ("fashion.inventory.events",
              "fashion.velocity.anomalies",
              "fashion.agent.responses"):
        try:
            schema = fetch_latest_schema(sr, t)
            print(f"  ok   {t}-value  ({len(schema)} bytes)")
        except RuntimeError as err:
            print(f"  FAIL {t}-value  {err}")
