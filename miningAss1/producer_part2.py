"""PART 2 DISCUSSION
Using Kafka in this setup makes the whole system more organized and efficient.
Instead of having every consumer call the API separately — which could overload it, cause inconsistent results, 
or fail if the API goes down — we have one producer that handles all communication with the API.
Kafka acts like a smart middle layer: it stores the incoming data and makes sure all consumers can get the same information, 
whenever they need it.
Each consumer reads the data at its own pace without affecting others, which makes the system much more flexible and stable.

Another big advantage is reliability.
Even if the API is temporarily unavailable, Kafka already has the data safely stored in the topic, 
so consumers can keep working without interruption.
It also supports scaling easily — if you need to process more data or add new consumers,
you can just connect them without changing how the API is accessed.
And thanks to the Schema Registry, every message follows a consistent structure, 
which keeps things clean and prevents errors when different parts of the system communicate.
"""


import os
import time
import requests
import certifi
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


for candidate in [
    Path(__file__).resolve().parent / ".env",
    Path(__file__).resolve().parent.parent / ".env",
]:
    if candidate.exists():
        load_dotenv(candidate)
        break
else:
    load_dotenv()

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
USER = os.getenv("SASL_USERNAME")
PASS = os.getenv("SASL_PASSWORD")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECH = os.getenv("SASL_MECHANISM", "PLAIN")
TOPIC = os.getenv("API_TOPIC", "AIN429_api_quotes")
FETCH_EVERY = float(os.getenv("API_FETCH_INTERVAL_SEC", "1.5"))
API_URL = os.getenv("API_URL", "https://api.quotable.io/random")

SR_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

required = {
    "BOOTSTRAP_SERVERS": BOOTSTRAP,
    "SASL_USERNAME": USER,
    "SASL_PASSWORD": PASS,
    "SCHEMA_REGISTRY_URL": SR_URL,
    "SCHEMA_REGISTRY_API_KEY": SR_KEY,
    "SCHEMA_REGISTRY_API_SECRET": SR_SECRET,
}

missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

BASE = Path(__file__).resolve().parent
SCHEMA_PATH = BASE / "schemas" / "quote_event_schema.json"
schema_str = SCHEMA_PATH.read_text(encoding="utf-8").strip()
if not schema_str:
    raise ValueError(f"Schema file is empty: {SCHEMA_PATH}")

sr = SchemaRegistryClient({"url": SR_URL, "basic.auth.user.info": f"{SR_KEY}:{SR_SECRET}"})
val_ser = JSONSerializer(schema_str=schema_str, schema_registry_client=sr, to_dict=lambda d, ctx: d)
key_ser = StringSerializer("utf_8")

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": SECURITY_PROTOCOL,
    "sasl.mechanism": SASL_MECH,
    "sasl.username": USER,
    "sasl.password": PASS,
    "linger.ms": 50,
    "acks": "all",
})

def delivery(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ {msg.topic()}[{msg.partition()}]@{msg.offset()} key={msg.key().decode() if msg.key() else None}")

def fetch_record():
    r = requests.get(API_URL, timeout=10, verify=certifi.where())
    r.raise_for_status()
    d = r.json()

    now = datetime.now(timezone.utc).isoformat()

    if "adviceslip.com" in API_URL:
        # { slip: { id, advice } }
        slip = d.get("slip", {})
        return {
            "quote_id":  str(slip.get("id") or ""),
            "content":   slip.get("advice"),
            "author":    "adviceslip",
            "tags":      [],
            "length":    None,
            "fetched_at": now
        }


    raise RuntimeError(f"Unsupported API_URL: {API_URL}")

def main():
    print(f"Producing API data to '{TOPIC}' (Ctrl+C to stop)")
    print(f"🔎 Using API_URL = {API_URL}")
    required_fields = ("quote_id", "content", "author", "fetched_at")

    try:
        while True:
            try:
                payload = fetch_record()
            except Exception as e:
                print(f"⚠️ Fetch failed: {e}")
                time.sleep(2.0)
                continue

            payload = {k: v for k, v in payload.items() if v is not None}

            if not all(payload.get(f) for f in required_fields):
                print(f"⚠️ Skipping invalid payload (missing required): {payload}")
                time.sleep(FETCH_EVERY)
                continue

            key = payload.get("quote_id") or payload.get("author") or "key"

            producer.produce(
                topic=TOPIC,
                key=key_ser(str(key), SerializationContext(TOPIC, MessageField.KEY)),
                value=val_ser(payload, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery
            )
            producer.poll(0)

            time.sleep(FETCH_EVERY)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush(10)

if __name__ == "__main__":
    main()