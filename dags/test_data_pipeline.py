import pytest
from google.cloud import storage
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import re
import io
import os

transformed_bucked_name  = "arkan-ecommerce-data-pipeline-transformed"
dateyesterday = (datetime.now().astimezone(timezone(timedelta(hours=7)))-timedelta(days=1)).strftime("%Y-%m-%d")
# dateyesterday = "2024-05-25"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/service_acc_key.json"

gcs = storage.Client()
blobs = gcs.list_blobs(bucket_or_name=transformed_bucked_name, prefix=dateyesterday)
blob = [n for n in blobs if re.match(r'.*\.csv$', n.name)][0]
data = blob.download_as_bytes()
df = pd.read_csv(io.BytesIO(data))

list_column = ["id","order_date","payment_gateway","shipping_code","shipping_province",
                "shipping_country","shipping_cost","customer_id","is_email_marketing",
                "is_sms_marketing","customer_province","customer_country","product_name",
                "product_variant","product_vendor","product_price","quantity",
                "total_product_price","total_tax_product"]

# check if column exist
def test_col_exist():
    for c in list_column:
        assert c in df.columns

# check for null
def test_null_check():
    for c in list_column:
        assert np.where(df[c].isnull())

# check data consistency
def test_datatype_col_id():
    assert (df['payment_gateway'].isin(['Cash on Delivery (COD)', 'bogus']).any())

def test_datatype_col_shipping_code():
    assert (df['shipping_code'].isin(['Express', 'Reguler']).any())

def test_datatype_col_is_email_marketing():
    assert (df['is_email_marketing'].isin([True, False]).any())

def test_datatype_col_is_sms_marketing():
    assert (df['is_sms_marketing'].isin([True, False]).any())

# usign airflow bash operator