import pandas as pd
import pytest
import boto3
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from io import StringIO


db_properties = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
}

sql_request = """
SELECT client_id, MAX(datetime::date) AS last_operation_date
FROM public.sales
GROUP BY client_id
"""

# В Github тесты будут валиться, поскольку БД была развернута локально, а креды к S3 - удалены из кода.

@pytest.fixture()
def engine():
    return create_engine(f'postgresql://{db_properties["user"]}:{db_properties["password"]}@{db_properties["host"]}:{db_properties["port"]}/{db_properties["database"]}')
    
@pytest.fixture()    
def s3():
    session = boto3.session.Session(aws_access_key_id="", 
                                    aws_secret_access_key="", 
                                    region_name="")
    return session.client(service_name="s3", endpoint_url="https://storage.yandexcloud.net")

@pytest.fixture()
def bucket():
    return 'rugrat-s3-cloud'

def test_database_connection(engine):
    try:
        engine.connect()
        pytest.success
    except SQLAlchemyError as err:
        pytest.fail(f'Connection to PostgreSQL failed: {str(err)}')

def test_database_request(engine):
    try:
        pd.read_sql('SELECT 1', engine)
        df = pd.read_sql('SELECT 1', engine)
        assert df is not None
        assert df.iloc[0, 0] == 1
    except Exception as e:
        pytest.fail(f'SQL request failed: {str(e)}')

def test_s3_bucket_avaliability(s3):
    try:
        buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        assert 'rugrat-s3-cloud' in buckets
    except Exception as e:
        pytest.fail(f'Bucket is not avaliable: {str(e)}')

def test_s3_upload_download(s3, bucket):
    try:
        txt_before = StringIO()
        txt_after_text = StringIO()

        txt_before_text = 'Hello World!\nS3 test in action.'
        txt_before.write(txt_before_text)
        s3.put_object(Bucket=bucket, Key='test.txt', Body=txt_before.getvalue())
        time.sleep(3)

        txt_after_text = s3.get_object(Bucket=bucket, Key="test.txt")['Body'].read().decode("utf-8")
        assert txt_before_text == txt_after_text

        time.sleep(3)
        rm_response = s3.delete_object(Bucket=bucket, Key='test.txt')
        assert rm_response['ResponseMetadata']['HTTPStatusCode'] == 204
    except Exception as e:
        pytest.fail(f'Upload-Download-Delete S3 process failed: {str(e)}')


def create_pg_connection():
    return create_engine(f'postgresql://{db_properties["user"]}:{db_properties["password"]}@{db_properties["host"]}:{db_properties["port"]}/{db_properties["database"]}')

def create_s3_session():
    session = boto3.session.Session(aws_access_key_id="", 
                                    aws_secret_access_key="", 
                                    region_name="")
    s3 = session.client(service_name="s3", endpoint_url="https://storage.yandexcloud.net")
    bucket = "rugrat-s3-cloud"
    return (s3, bucket)


def main():
    engine = create_pg_connection()
    csv_buffer = StringIO()
    df = pd.read_sql(sql_request, engine)
    df.to_csv(csv_buffer)
    s3, bucket = create_s3_session()
    s3.put_object(Bucket=bucket, Key="sales.csv", Body=csv_buffer.getvalue())
