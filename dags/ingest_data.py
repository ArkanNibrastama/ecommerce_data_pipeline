import creds
import requests
from datetime import datetime, timedelta
import json
from google.cloud import storage
import os

dateyesterday = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
# dateyesterday = "2024-05-25"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/service_acc_key.json"

gcs = storage.Client()
raw_bucket_name = "arkan-ecommerce-data-pipeline-raw"
bucket = gcs.bucket(raw_bucket_name)

def create_session():

    session = requests.Session()
    session.headers.update({
        "X-Shopify-Access-Token": creds.token
    })

    return session

def ingest(start_date, end_date):
    store = create_session()
    orders = store.get(creds.url+"/admin/api/"+creds.api_version+"/orders.json?status=any&created_at_min="+start_date+"&created_at_max="+end_date)
    order_json = orders.json()

    # return semua order
    for i, order in enumerate(order_json['orders']):

        blob = bucket.blob(dateyesterday+"_"+str(i)+'.json')
        blob.upload_from_string(json.dumps(order))


def main():
    start_date = f'{dateyesterday}T00:00:00%2B07:00'
    end_date = f'{dateyesterday}T23:59:59%2B07:00'
    ingest(start_date=start_date, end_date=end_date)