""" PART 2 DISCUSSION
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


import os, sys, time
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

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
API_TOPIC = os.getenv("API_TOPIC", "AIN429_api_quotes")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")

if not BOOTSTRAP:
    print("❌ BOOTSTRAP_SERVERS can not read")
    sys.exit(1)

print(f"ℹ️ BOOTSTRAP_SERVERS={BOOTSTRAP}")
print(f"ℹ️ API_TOPIC={API_TOPIC}")

admin_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": SECURITY_PROTOCOL,
    "sasl.mechanism": SASL_MECHANISM,
    "sasl.username": SASL_USERNAME,
    "sasl.password": SASL_PASSWORD,
}

admin = AdminClient(admin_conf)

try:
    md = admin.list_topics(timeout=10)
    print(f"✅ Connected. cluster_id={md.cluster_id} brokers={list(md.brokers.keys())}")
except Exception as e:
    print(f"❌ list_topics failed: {e}")
    sys.exit(1)

new_topic = NewTopic(API_TOPIC, num_partitions=3, replication_factor=3)
fs = admin.create_topics([new_topic])
try:
    fs[API_TOPIC].result()
    print(f"✅ Topic created: {API_TOPIC}")
except Exception as e:
    print(f"ℹ️ Create returned: {e}")

