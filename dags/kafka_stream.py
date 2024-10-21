import time
import logging
import uuid
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class User(BaseModel):
    id: uuid.UUID
    first_name: str
    last_name: str
    email: str
    username: str
    dob: datetime
    registered_date: datetime
    phone: str
    address: str
    gender: str
    postcode: str
    picture: str

    @classmethod
    def from_api(cls, response: dict) -> "User":
        location = response["location"]
        return cls(
            id=uuid.uuid4(),
            first_name=response["name"]["first"],
            last_name=response["name"]["last"],
            gender=response["gender"],
            address=f"{location['street']['number']} {location['street']['name']}, "
            f"{location['city']}, {location['state']}, {location['country']}",
            postcode=str(location["postcode"]),
            email=response["email"],
            username=response["login"]["username"],
            dob=response["dob"]["date"],
            registered_date=response["registered"]["date"],
            phone=response["phone"],
            picture=response["picture"]["medium"],
        )


class DagSettings(BaseSettings):
    owner: str = "airflow"
    start_date: datetime = datetime(2024, 1, 1)


default_args = DagSettings()


def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res["results"][0]


def format_data(res: dict) -> User:
    return User.from_api(res)


def stream_data():
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send("users_created", res.model_dump_json().encode("utf-8"))
            time.sleep(5)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

with DAG(
    "user_automation",
    default_args=default_args.model_dump(),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    stream_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data,
    )
