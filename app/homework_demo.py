import json
import requests

BASE_URL = "http://127.0.0.1:8000"


def pretty_print(title: str, response: requests.Response) -> dict:
    print(f"\n===== {title} =====")
    print("Status Code:", response.status_code)
    try:
        data = response.json()
        print(json.dumps(data, indent=2))
        return data
    except Exception:
        print(response.text)
        return {}


def main() -> None:
    create_payload = {
        "operation": "CREATE_USER",
        "name": "Alice",
        "email": "alice@test.com",
        "age": 25,
    }

    create_response = requests.post(
        f"{BASE_URL}/users",
        json=create_payload,
        timeout=20,
    )
    create_data = pretty_print("CREATE_USER", create_response)

    if not create_data.get("success"):
        print("\nCREATE_USER failed, so GET_USER cannot continue.")
        return

    user_id = create_data["userId"]

    get_payload = {
        "operation": "GET_USER",
        "userId": user_id,
    }

    get_response = requests.post(
        f"{BASE_URL}/users",
        json=get_payload,
        timeout=20,
    )
    pretty_print("GET_USER", get_response)


if __name__ == "__main__":
    main()