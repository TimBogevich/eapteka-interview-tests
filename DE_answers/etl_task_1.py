import pandas as pd
from sqlalchemy import create_engine

'''
Создание таблицы в PostgreSQL

CREATE TABLE public.sales (
	datetime timestamp NULL,
	client_id int8 NULL,
	order_id int8 NULL,
	item_id int8 NULL,
	amount int8 NULL,
	"cost" float8 NULL,
	vat float8 NULL,
	return_flag text NULL
);
'''

# Файлы объемом 100 гб лучше обрабатывать с помощью PySpark, но в данном случае пример реализован с помощью Pandas

db_properties = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
    "database": "postgres"
}

file_path = r"C:\Users\Rugratko\Documents\Github\eapteka-interview-tests\DE\csv\sales.csv"

def main():
    engine = create_engine(f'postgresql://{db_properties["user"]}:{db_properties["password"]}@{db_properties["host"]}:{db_properties["port"]}/{db_properties["database"]}')
    with pd.read_csv(file_path, sep=',', chunksize=10000) as reader:
        for chunk in reader:
            chunk.to_sql("sales", engine, index=False)