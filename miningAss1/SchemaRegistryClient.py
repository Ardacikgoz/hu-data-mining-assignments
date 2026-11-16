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
from dotenv import load_dotenv
from pathlib import Path
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

load_dotenv()
SR_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")
API_TOPIC = os.getenv("API_TOPIC", "AIN429_api_quotes")

sr = SchemaRegistryClient({
    "url": SR_URL,
    "basic.auth.user.info": f"{SR_KEY}:{SR_SECRET}",
})

schema_path = Path(__file__).resolve().parent / "schemas" / "quote_event_schema.json"
schema_str = schema_path.read_text(encoding="utf-8")
subject = f"{API_TOPIC}-value"
schema = Schema(schema_str=schema_str, schema_type="JSON")

try:
    registered = sr.register_schema(subject, schema)
    schema_id = getattr(registered, "id", None) or getattr(registered, "schema_id", None) or (registered if isinstance(registered, int) else None)
    print(f"✅ Registered schema subject={subject} id={schema_id}")
except Exception as e:
    print(f"ℹ️ Could not register: {e}")
    latest = sr.get_latest_version(subject)
    print(f"ℹ️ Latest schema id={latest.schema_id} subject={subject}")
