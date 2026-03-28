import json

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "drafts"
OUTPUT_TOPIC = "final"


def review_draft(draft: str) -> tuple[str, str]:
    cleaned = draft.strip()

    if not cleaned:
        return "rejected", "Draft was empty."

    return "approved", cleaned


def main() -> None:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="reviewer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("[Reviewer] Listening on topic: drafts")

    for record in consumer:
        message = record.value
        question_id = message.get("question_id")
        draft = message.get("draft", "")

        print(f"[Reviewer] Received from drafts: {json.dumps(message)}")

        if not question_id:
            print("[Reviewer] Skipping invalid message")
            continue

        status, final_answer = review_draft(draft)

        final_message = {
            "question_id": question_id,
            "status": status,
            "answer": final_answer,
        }

        producer.send(OUTPUT_TOPIC, final_message).get(timeout=10)
        print(f"[Reviewer] Sent to final: {json.dumps(final_message)}")


if __name__ == "__main__":
    main()