from datetime import datetime
import json

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pymongo
from pycoingecko import CoinGeckoAPI


def load_cryptocurencies(names):
    cg = CoinGeckoAPI()
    prices = cg.get_price(names, vs_currencies='usd')
    print(prices)
    return prices

def check_collection_exists(name):
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client.crypto_data
    if name in db.list_collection_names():
         return "create_collection_{}".format(name)
    return "insert_{}_price".format(name)


def create_collection(name):
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client.crypto_data
    db.create_collection("{}_price".format(name=name), timeseries={"timeField": "timestamp", "granularity": "minutes"})


def insert_row(**context):
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client.crypto_data
    name = context["templates_dict"]["name"]
    collection = db["{}_price".format(name)]
    collection.insert_one({
        "timestamp": datetime.utcnow(),
        "price": json.loads(context["templates_dict"]["price"].replace("'", "\""))[name]["usd"]
    })
    


with DAG(
    dag_id="load_data",
    schedule_interval='*/1 * * * *',
    #schedule_interval=None,
    start_date=datetime(2022, 1, 28, 10),
) as dag:
    cryptocurencies = ['bitcoin', 'ethereum']
    prices = PythonOperator(task_id="load_prices_data", python_callable=load_cryptocurencies, op_kwargs={"names": cryptocurencies})
    for cryptocurency in cryptocurencies:
        create_collection_if_not_exist = PythonOperator(task_id="create_collection_{}".format(cryptocurency), python_callable=create_collection, op_kwargs={
                                              "name": cryptocurency
                                          })
        insert = PythonOperator(task_id="insert_{}_price".format(cryptocurency), 
                                python_callable=insert_row,
                                trigger_rule="all_done",
                                provide_context=True, 
                                templates_dict={"name": cryptocurency, "price": "{{ti.xcom_pull(task_ids='load_prices_data')}}"})
        check_if_collection_exists = BranchPythonOperator(task_id="check_collection_{}_exists".format(cryptocurency), 
                                       python_callable=check_collection_exists, 
                                       op_kwargs={"name": cryptocurency})
        prices >> check_if_collection_exists >> [insert, create_collection_if_not_exist]
        create_collection_if_not_exist >> insert
