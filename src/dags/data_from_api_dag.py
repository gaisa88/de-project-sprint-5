import logging
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import requests
import json
import psycopg2
log = logging.getLogger(__name__)

nickname = 'egor'
cohort = '24'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key
    }


conn = psycopg2.connect(
    dbname='de',
    user='jovyan',
    password='jovyan',
    host='localhost',
    port='5432'
)


def get_couriers_from_api(sort_field='_id', sort_direction='asc', limit=50):
    couriers_url = base_url + 'couriers'
    all_couriers = []
    offset = 0

    while True:
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset
        }
        response = requests.get(couriers_url, headers=headers, params=params)
        couriers = response.json()

        if not couriers:
            break

        all_couriers.extend(couriers)

        if len(couriers) < limit:
            break

        offset += limit

    return all_couriers

def couriers_from_api_to_stg():
    couriers_list = get_couriers_from_api()
    for courier_json in couriers_list:
        courier_id = courier_json['_id']
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO stg.api_couriers (object_id, object_value)
                    VALUES(%(courier_id)s, %(courier_json)s::json)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_id = EXCLUDED.object_id,
                        object_value = EXCLUDED.object_value;
                """,{
                        "courier_id": courier_id,
                    "courier_json": json.dumps(courier_json, ensure_ascii=False)
                    }
                        )        
    conn.commit()


#Функция get_deliveries_from_api возвращает данные о доставках из API системы доставки заказов
def get_deliveries_from_api(restaurant_id=None, from_date=None, to_date=None, sort_field='_id'):
    deliveries_url = base_url + 'deliveries'
    all_deliveries = []
    offset = 0
    limit = 50

    while True:
        params = {
            'restaurant_id': restaurant_id,
            'from_date': from_date,
            'to_date': to_date,
            'sort_field': sort_field,
            'limit': limit,
            'offset': offset
        }
        response = requests.get(deliveries_url, headers=headers, params=params)
        deliveries = response.json()

        if not deliveries:
            break

        all_deliveries.extend(deliveries)

        if len(deliveries) < limit:
            break

        offset += limit

    return all_deliveries

def deliveries_from_api_to_stg():
    deliveries_list= get_deliveries_from_api()
    for delivery_json in deliveries_list:
        delivery_id = delivery_json['delivery_id']
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO stg.api_deliveries (object_id, object_value)
                VALUES(%(delivery_id)s, %(delivery_json)s::json)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_id = EXCLUDED.object_id,
                    object_value = EXCLUDED.object_value;
                """, {
                        "delivery_id": delivery_id,
                        "delivery_json": json.dumps(delivery_json, ensure_ascii=False)
                    })
    conn.commit()


dag = DAG(dag_id='data_from_api_to_stg_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['form couriers and deliveries api to stg'],
    is_paused_upon_creation=False)

task_1 = PythonOperator(task_id='couriers_from_api_to_stg',
                      python_callable=couriers_from_api_to_stg,
                      dag=dag)   

task_2 = PythonOperator(task_id='deliveries_from_api_to_stg',
                      python_callable=deliveries_from_api_to_stg,
                      dag=dag)   

task_1 >> task_2