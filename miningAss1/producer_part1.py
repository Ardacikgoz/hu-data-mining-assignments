import os, random, time
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")
TOPIC = os.getenv("TOPIC", "basket_match")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

# ----- Şema yükle -----
BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schemas" / "basket_event_schema.json"
schema_str = SCHEMA_PATH.read_text(encoding="utf-8").strip()
if not schema_str:
    raise ValueError(f"Schema file is empty: {SCHEMA_PATH}")

# ----- SR client ve JSON serializer -----
sr_client = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}",
})
json_serializer = JSONSerializer(schema_str=schema_str, schema_registry_client=sr_client, to_dict=lambda d, ctx: d)
key_serializer = StringSerializer("utf_8")

# ----- Kafka producer -----
producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": SECURITY_PROTOCOL,
    "sasl.mechanism": SASL_MECHANISM,
    "sasl.username": SASL_USERNAME,
    "sasl.password": SASL_PASSWORD,
    "linger.ms": 50,
    "acks": "all"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ {msg.topic()}[{msg.partition()}]@{msg.offset()} key={msg.key().decode() if msg.key() else None}")

# ----- Maç verisi -----
TEAMS = {
    "Fenerbahce Beko": ["Hall", "Tucker", "Biberovic", "Jantunen", "Birch"],
    "Anadolu Efes":    ["Larkin", "Cordinier", "Weiler-Babb", "Osmani", "Smits"]
}
TOTALS = { "Fenerbahce Beko": 0, "Anadolu Efes": 0 }
MATCH_ID = "FBK-EFES-2025"

def make_basket_event():
    team = random.choice(list(TEAMS.keys()))
    player = random.choice(TEAMS[team])
    pts = random.choice([1, 2, 3])
    TOTALS[team] += pts
    return {
        "match_id": MATCH_ID,
        "team": team,
        "player": player,
        "points_scored": pts,
        "total_team_score": TOTALS[team],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }, team  

def main():
    print(f"Producing basketball events to '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            value, team_key = make_basket_event()
            team_key = f"{value['match_id']}|{value['team']}|{value['player']}" 
            producer.produce(
                topic=TOPIC,
                key=key_serializer(team_key, SerializationContext(TOPIC, MessageField.KEY)),
                value=json_serializer(value, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery_report
            )
            producer.poll(0)
            time.sleep(1.0)  # ~1 event/sn
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush(10)

if __name__ == "__main__":
    main()