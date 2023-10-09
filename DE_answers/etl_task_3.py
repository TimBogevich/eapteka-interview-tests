import pandas as pd
import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine


db_properties = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
}


def main():
    engine = create_engine(
        f'postgresql://{db_properties["user"]}:{db_properties["password"]}@{db_properties["host"]}:{db_properties["port"]}/{db_properties["database"]}'
    )

    try:
        consumer = KafkaConsumer(
            group_id="python_consumer",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        consumer.subscribe(["messages"])
        df = pd.DataFrame(
            [message.value for message in consumer],
        )

    except Exception as e:
        print(e)

    finally:
        consumer.close(autocommit=False)

    df.astype(str).to_sql("users", engine, index=False, if_exists="replace")
