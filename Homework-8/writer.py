import json

from kafka import KafkaConsumer, KafkaProducer
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "tasks"
OUTPUT_TOPIC = "drafts"


def generate_short_answer_from_prompt(prompt_value) -> str:
    prompt_text = prompt_value.to_string()

    question = ""
    for line in prompt_text.splitlines():
        if line.startswith("Question:"):
            question = line.replace("Question:", "", 1).strip()
            break

    q = question.lower()

    if "kafka" in q:
        return (
            "Kafka is a distributed event streaming platform used to move messages "
            "between systems reliably and at scale. It helps decouple services and "
            "supports real-time communication through topics."
        )

    if "langchain" in q:
        return (
            "LangChain helps build LLM-powered workflows by connecting prompts, models, "
            "and logic into chains. It is useful for building structured agent systems."
        )

    return (
        f"{question.rstrip('?')} can be answered clearly by focusing on the main idea first. "
        f"This draft is short, direct, and follows the planner's steps."
    )


def build_chain():
    prompt = ChatPromptTemplate.from_template(
        "You are the Writer agent.\n"
        "Question: {question}\n"
        "Plan:\n{plan_text}\n"
        "Write a short answer."
    )

    chain = prompt | RunnableLambda(generate_short_answer_from_prompt) | StrOutputParser()
    return chain


def main() -> None:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="writer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    chain = build_chain()

    print("[Writer] Listening on topic: tasks")

    for record in consumer:
        message = record.value
        question_id = message.get("question_id")
        question = message.get("question", "").strip()
        plan = message.get("plan", [])

        print(f"[Writer] Received from tasks: {json.dumps(message)}")

        if not question_id or not question:
            print("[Writer] Skipping invalid message")
            continue

        answer = chain.invoke(
            {
                "question": question,
                "plan_text": "\n".join(f"- {step}" for step in plan),
            }
        )

        draft_message = {
            "question_id": question_id,
            "question": question,
            "plan": plan,
            "draft": answer,
        }

        producer.send(OUTPUT_TOPIC, draft_message).get(timeout=10)
        print(f"[Writer] Sent to drafts: {json.dumps(draft_message)}")


if __name__ == "__main__":
    main()