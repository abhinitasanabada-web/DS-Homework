import json
import uuid
from typing import Any

from email_validator import EmailNotValidError, validate_email
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
REQUEST_TOPIC = "user_requests"
DEFAULT_REPLY_TOPIC = "user_responses"

# In-memory user store required by rubric
users: dict[str, dict[str, Any]] = {}


def make_response(correlation_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "correlation_id": correlation_id,
        "data": payload,
    }


def validate_create_user(data: dict[str, Any]) -> tuple[bool, dict[str, Any] | str]:
    name = data.get("name")
    email = data.get("email")
    age = data.get("age")

    if not name or not isinstance(name, str):
        return False, "name is required and must be a string"

    if not email or not isinstance(email, str):
        return False, "email is required and must be a string"

    try:
        validate_email(email, check_deliverability=False)
    except EmailNotValidError:
        return False, "invalid email format"

    if age is None:
        return False, "age is required"

    try:
        age = int(age)
    except (TypeError, ValueError):
        return False, "age must be a number"

    if age <= 0:
        return False, "age must be greater than 0"

    return True, {
        "name": name.strip(),
        "email": email.strip(),
        "age": age,
    }


def process_request(message: dict[str, Any]) -> dict[str, Any]:
    correlation_id = message.get("correlation_id", "")
    data = message.get("data", {})

    operation = data.get("operation")

    if operation == "CREATE_USER":
        valid, result = validate_create_user(data)
        if not valid:
            return make_response(
                correlation_id,
                {
                    "success": False,
                    "message": result,
                },
            )

        user_id = str(uuid.uuid4())
        users[user_id] = {
            "userId": user_id,
            "name": result["name"],
            "email": result["email"],
            "age": result["age"],
        }

        return make_response(
            correlation_id,
            {
                "success": True,
                "userId": user_id,
                "message": "User created",
            },
        )

    if operation == "GET_USER":
        user_id = data.get("userId")
        if not user_id:
            return make_response(
                correlation_id,
                {
                    "success": False,
                    "message": "userId is required",
                },
            )

        user = users.get(user_id)
        if not user:
            return make_response(
                correlation_id,
                {
                    "success": False,
                    "message": "User not found",
                },
            )

        return make_response(
            correlation_id,
            {
                "success": True,
                "user": user,
            },
        )

    return make_response(
        correlation_id,
        {
            "success": False,
            "message": "Invalid operation",
        },
    )


def main() -> None:
    consumer = KafkaConsumer(
        REQUEST_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="homework8-worker-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Consumer is ready and listening...")

    for record in consumer:
        incoming_message = record.value
        correlation_id = incoming_message.get("correlation_id", "")
        reply_to = incoming_message.get("reply_to", DEFAULT_REPLY_TOPIC)

        print(
            f"[Worker] Message received from {REQUEST_TOPIC}: "
            f"{json.dumps(incoming_message)}"
        )

        response_message = process_request(incoming_message)

        print(
            f"[Worker] Message processed for correlation_id={correlation_id}: "
            f"{json.dumps(response_message['data'])}"
        )

        producer.send(reply_to, response_message).get(timeout=10)

        print(
            f"[Worker] Response sent to {reply_to}: "
            f"{json.dumps(response_message)}"
        )


if __name__ == "__main__":
    main()