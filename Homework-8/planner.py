import json

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "inbox"
OUTPUT_TOPIC = "tasks"


def build_plan(question: str) -> list[str]:
    return [
        "Understand the user question",
        "Identify the key topic and intent",
        "Prepare a short and clear answer",
    ]


def main() -> None:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="planner-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("[Planner] Listening on topic: inbox")

    for record in consumer:
        message = record.value
        question_id = message.get("question_id")
        question = message.get("question", "").strip()

        print(f"[Planner] Received from inbox: {json.dumps(message)}")

        if not question_id or not question:
            print("[Planner] Skipping invalid message")
            continue

        task_message = {
            "question_id": question_id,
            "question": question,
            "plan": build_plan(question),
        }

        producer.send(OUTPUT_TOPIC, task_message).get(timeout=10)
        print(f"[Planner] Sent to tasks: {json.dumps(task_message)}")


if __name__ == "__main__":
    main()