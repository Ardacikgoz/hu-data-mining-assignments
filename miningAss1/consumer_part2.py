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


import os, json
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

load_dotenv()
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
USER = os.getenv("SASL_USERNAME")
PASS = os.getenv("SASL_PASSWORD")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECH = os.getenv("SASL_MECHANISM", "PLAIN")
TOPIC = os.getenv("API_TOPIC", "AIN429_api_quotes")
GROUP = os.getenv("GROUP_ID", "ain429-api-consumer")

SR_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

sr = SchemaRegistryClient({"url": SR_URL, "basic.auth.user.info": f"{SR_KEY}:{SR_SECRET}"})
deser = JSONDeserializer(schema_str=None, from_dict=lambda d, ctx: d, schema_registry_client=sr)

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": SECURITY_PROTOCOL,
    "sasl.mechanism": SASL_MECH,
    "sasl.username": USER,
    "sasl.password": PASS,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

def main():
    c = Consumer(conf)
    c.subscribe([TOPIC])
    print(f"Consuming from '{TOPIC}' as '{GROUP}' (Ctrl+C to stop)")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            key = msg.key().decode("utf-8") if msg.key() else None
            val = deser(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
            print(f"[p={msg.partition()} off={msg.offset()}] key={key} value={json.dumps(val, ensure_ascii=False)}")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        c.close()

if __name__ == "__main__":
    main()