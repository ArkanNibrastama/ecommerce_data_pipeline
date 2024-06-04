from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField,  StructType, IntegerType, LongType, TimestampType, BooleanType
import os
from datetime import datetime, timedelta, timezone
import re
from google.cloud import storage
import json

spark = SparkSession.builder\
        .appName("data_transformation")\
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")\
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/service_acc_key.json")\
        .getOrCreate()

empty_RDD = spark.sparkContext.emptyRDD()

schema = StructType([
    StructField('id', StringType(), True), #->[done] transform into string
    StructField('order_date', TimestampType(), True), #-> [done] transform into datetime / timestamp
    StructField('payment_gateway', StringType(), True),
    StructField('shipping_code', StringType(), True),
    StructField('shipping_province', StringType(), True),
    StructField('shipping_country', StringType(), True),
    StructField('shipping_cost', LongType(), True), #->[done] transform into integer / long
    StructField('customer_id', StringType(), True), #->[done] transform into string
    StructField('is_email_marketing', BooleanType(), True), #->[done] transform into boolean
    StructField('is_sms_marketing', BooleanType(), True), #->[done] transform into boolean
    StructField('customer_province', StringType(), True),
    StructField('customer_country', StringType(), True),
    StructField('product_name', StringType(), True),
    StructField('product_variant', StringType(), True),
    StructField('product_vendor', StringType(), True),
    StructField('product_price', LongType(), True), #->[done] transform into long
    StructField('quantity', IntegerType(), True), #->[done] transform into integer
    StructField('total_product_price', LongType(), True),
    StructField('total_tax_product', LongType(), True)
])

df = spark.createDataFrame(data=empty_RDD, schema=schema)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/service_acc_key.json"

gcs = storage.Client()
raw_bucket_name = "arkan-ecommerce-data-pipeline-raw"
transformed_bucked_name  = "arkan-ecommerce-data-pipeline-transformed"

dateyesterday = (datetime.now().astimezone(timezone(timedelta(hours=7)))-timedelta(days=1)).strftime("%Y-%m-%d")
# dateyesterday = "2024-05-25"

json_files = [js.name for js in gcs.list_blobs(raw_bucket_name) if re.match(rf"{dateyesterday}_.\.json", js.name)]
for json_file in json_files:
    blob_files = f"gs://{raw_bucket_name}/{json_file}"
    df1 = spark.read.format('json').load(blob_files)

    products = df1.select(f.explode('line_items'))
    for c,i in enumerate(products.collect()):

        df2 = df1.select(
            f.col('id').alias('id'), 
            f.col('created_at').alias('order_date'),
            f.col('payment_gateway_names').getItem(0).alias('payment_gateway'),
            f.col('shipping_lines').getItem(0).getItem('code').alias('shipping_code'),
            f.col('shipping_address').getItem('province').alias('shipping_province'),
            f.col('shipping_address').getItem('country').alias('shipping_country'),
            f.col('shipping_lines').getItem(0).getItem('price').alias('shipping_cost'),
            f.col('customer').getItem('id').alias('customer_id'),
            f.col('customer').getItem('email_marketing_consent').getItem('state').alias('is_email_marketing'),
            f.col('customer').getItem('sms_marketing_consent').alias('is_sms_marketing'),
            f.col('customer').getItem('default_address').getItem('province').alias('customer_province'),
            f.col('customer').getItem('default_address').getItem('country').alias('customer_country'),
            f.col('line_items').getItem(c).getItem('title').alias('product_name'),
            f.col('line_items').getItem(c).getItem('variant_title').alias('product_variant'),
            f.col('line_items').getItem(c).getItem('vendor').alias('product_vendor'),
            f.col('line_items').getItem(c).getItem('price').alias('product_price'),
            f.col('line_items').getItem(c).getItem('quantity').alias('quantity'),
            f.col('line_items').getItem(c).getItem('tax_lines').getItem(0).getItem('price').alias('total_tax_product_old')
        )

        # transform below!
        #nanti df transform itu dijadiin satu aja
        #casting
        df3 = df2.withColumn('id', f.col('id').cast(StringType())).\
                withColumn('order_date', f.to_timestamp('order_date')).\
                withColumn('product_price', f.col('product_price').cast(LongType())).\
                withColumn('shipping_cost', f.col('shipping_cost').cast(LongType())).\
                withColumn('customer_id', f.col('customer_id').cast(StringType())).\
                withColumn('quantity', f.col('quantity').cast(IntegerType()))
        #boolean condition
        df4 = df3.withColumn('is_email_marketing', f.when(f.col('is_email_marketing').isNotNull(), True).otherwise(False)).\
                withColumn('is_sms_marketing', f.when(f.col('is_sms_marketing').isNotNull(), True).otherwise(False))

        #total (math operation)
        df5 = df4.withColumn('total_product_price', (f.col('product_price')*f.col('quantity'))).\
                withColumn('total_tax_product', f.col('total_tax_product_old').cast(LongType())).\
                drop('total_tax_product_old') # dorp karena spark itu ngisi value colomn nya urutan, buka sesuai nama

        #end transfrom

        df = df.union(df5)

# df.show()
df.coalesce(1).write.option('header', True).mode('overwrite').csv(f"gs://{transformed_bucked_name}/{dateyesterday}")