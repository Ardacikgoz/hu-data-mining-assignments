import os, json
from dotenv import load_dotenv

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")
TOPIC = os.getenv("TOPIC", "basket_match")
GROUP_ID = os.getenv("GROUP_ID", "basket-stats-consumer")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

sr_client = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}",
})
deserializer = JSONDeserializer(schema_str=None, from_dict=lambda d, ctx: d, schema_registry_client=sr_client)

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": SECURITY_PROTOCOL,
    "sasl.mechanism": SASL_MECHANISM,
    "sasl.username": SASL_USERNAME,
    "sasl.password": SASL_PASSWORD,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
}

def main():
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    print(f"Consuming from topic '{TOPIC}' as group '{GROUP_ID}'. Press Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            value = deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
            print(f"[p={msg.partition()} off={msg.offset()}] key={key} value={json.dumps(value, ensure_ascii=False)}")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()