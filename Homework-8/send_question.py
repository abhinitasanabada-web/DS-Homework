import json
import uuid

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
OUTPUT_TOPIC = "inbox"


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    message = {
        "question_id": str(uuid.uuid4()),
        "question": "What is Kafka and why is it useful?",
    }

    producer.send(OUTPUT_TOPIC, message).get(timeout=10)
    print(f"[Sender] Sent to inbox: {json.dumps(message)}")

    producer.close()


if __name__ == "__main__":
    main()