import json
import time
import uuid
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel

BOOTSTRAP_SERVERS = "localhost:9092"
REQUEST_TOPIC = "user_requests"
RESPONSE_TOPIC = "user_responses"

app = FastAPI(title="Homework 8 Part 1 - Kafka User Service")


class UserOperation(BaseModel):
    operation: str
    name: Optional[str] = None
    email: Optional[str] = None
    age: Optional[int] = None
    userId: Optional[str] = None


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def create_response_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=f"fastapi-response-{uuid.uuid4()}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    consumer.subscribe([topic])

    deadline = time.time() + 5
    while time.time() < deadline:
        consumer.poll(timeout_ms=200)
        if consumer.assignment():
            break

    return consumer


def send_request_and_wait(request_data: dict[str, Any]) -> dict[str, Any]:
    correlation_id = str(uuid.uuid4())
    message = {
        "correlation_id": correlation_id,
        "reply_to": RESPONSE_TOPIC,
        "data": request_data,
    }

    consumer = create_response_consumer(RESPONSE_TOPIC)
    producer = create_producer()

    try:
        print(
            f"[FastAPI] Message sent to {REQUEST_TOPIC} topic: "
            f"{json.dumps(message)}"
        )
        producer.send(REQUEST_TOPIC, message).get(timeout=10)

        deadline = time.time() + 15

        while time.time() < deadline:
            polled = consumer.poll(timeout_ms=500)

            for _topic_partition, records in polled.items():
                for record in records:
                    response = record.value
                    if response.get("correlation_id") == correlation_id:
                        print(
                            f"[FastAPI] Response received from {RESPONSE_TOPIC} "
                            f"topic and returned to client: {json.dumps(response)}"
                        )
                        return response.get("data", {})

        raise HTTPException(status_code=504, detail="Timed out waiting for Kafka response")

    finally:
        consumer.close()
        producer.close()


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "Homework 8 Part 1 FastAPI service is running"}


@app.post("/users")
def users_endpoint(payload: UserOperation) -> dict[str, Any]:
    return send_request_and_wait(payload.model_dump(exclude_none=True))