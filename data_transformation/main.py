from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField,  StructType, IntegerType, LongType

# https://spark.apache.org/docs/1.2.0/spark-standalone.html

spark = SparkSession.builder.master("spark://10.227.33.236:7077").appName("tes").getOrCreate()

empty_RDD = spark.sparkContext.emptyRDD()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('order_date', StringType(), True),
    StructField('product', StringType(), True)
])

df = spark.createDataFrame(data=empty_RDD, schema=schema)

input_json = spark.read.json("../data/01052024_2.json")
# input_json.printSchema()

products = input_json.select(f.explode('line_items'))
for c,i in enumerate(products.collect()):
    # print(c, i)
    df_prod = input_json.select(
        f.col('id').alias('id'), 
        f.col('created_at').alias('order_date'),
        # f.col('payment_gateway_names').getItem(0).alias('payment_gateway'),
        # f.col('shipping_lines').getItem(0).getItem('code').alias('shipping_code'),
        # f.col('shipping_address').getItem('province').alias('shipping_province'),
        # f.col('shipping_address').getItem('country').alias('shipping_country'),
        # f.col('shipping_lines').getItem(0).getItem('price').alias('shipping_cost'),
        # f.col('customer').getItem('id').alias('customer_id'),
        # f.col('customer').getItem('email_marketing_consent').getItem('state').alias('is_email_marketing'),
        # f.col('customer').getItem('sms_marketing_consent'),
        # f.col('customer').getItem('default_address').getItem('province').alias('customer_province'),
        # f.col('customer').getItem('default_address').getItem('country').alias('customer_country'),
        f.col('line_items').getItem(c).getItem('title').alias('product')
    )

    df = df.union(df_prod)

df.show()