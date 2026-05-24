from datetime import datetime, timedelta
import json
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
import boto3



# Настройка для все задач в Dag

default_args = {
    'owner': 'Sergey',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='gecko_coins_dag',
    description='Fetching data of crypto coins and load to Yandex S3',
    default_args=default_args,
    start_date=datetime(2026, 5, 23),
    schedule='@hourly',
    catchup=False,
    tags=['crypto', 'etl', 'yandex', 'gecko']
)
def cryptocoins_etl():


    api = Variable.get('API')


    @task()
    def extract() -> list:

        try:

            response = requests.get(api)
            response.raise_for_status()
            data = response.json()
            return data


        except Exception as e:
            raise Exception(f'Api is not working. Error: {e}')


    @task()
    def transform(data: list) -> list:
        processed = []

        for coin in data:
            processed.append({
                'id': coin['id'],
                'symbol': coin['symbol'],
                'name': coin['name'],
                'current_price': coin['current_price'],
                'market_cap': coin['market_cap'],
                'market_cap_rank': coin['market_cap_rank'],
                'fully_diluted_valuation': coin['fully_diluted_valuation'],
                'total_volume': coin['total_volume'],
                'high_24h': coin['high_24h'],
                'low_24h': coin['low_24h'],
                'price_change_24h': coin['price_change_24h'],
                'price_change_percentage_24h': coin['price_change_percentage_24h'],
                'market_cap_change_24h': coin['market_cap_change_24h'],
                'market_cap_change_percentage_24h': coin['market_cap_change_percentage_24h'],
                'circulating_supply': coin['circulating_supply'],
                'total_supply': coin['total_supply'],
                'max_supply': coin['max_supply'],
                'ath': coin['ath'],
                'ath_change_percentage': coin['ath_change_percentage'],
                'ath_date': coin['ath_date'],
                'atl': coin['atl'],
                'atl_change_percentage': coin['atl_change_percentage'],
                'atl_date': coin['atl_date'],
                'roi': coin['roi'],
                'last_updated': coin['last_updated']
            })

        return processed

    @task()
    def load_to_s3(data: list) -> None:

        access_key = Variable.get('YC_ACCESS_KEY')
        secret_key = Variable.get('YC_SEC_KEY')
        bucket = Variable.get('YC_BUCKET')

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H-%M-%S")


        key = f"crypto-data/date={current_date}/markets_{current_time}.json"

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False),
            ContentType='application/json',
        )
        print(f'Loaded {len(data)} coins to S3: {key}')

        print(repr(access_key))
        print(repr(secret_key))
        print(repr(bucket))

        response1 = s3.list_buckets()
        print(response1)

    raw = extract()
    clean = transform(raw)
    load_to_s3(clean)

cryptocoins_etl()