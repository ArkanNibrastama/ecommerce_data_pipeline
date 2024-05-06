from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField,  StructType, IntegerType, LongType, TimestampType, BooleanType

# https://spark.apache.org/docs/1.2.0/spark-standalone.html

spark = SparkSession.builder.master("spark://192.168.31.56:7077").appName("tes").getOrCreate()

empty_RDD = spark.sparkContext.emptyRDD()

schema = StructType([
    StructField('id', IntegerType(), True), #->[done] transform into string
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

input_json = spark.read.json("../data/01052024_2.json")
# input_json.printSchema()

products = input_json.select(f.explode('line_items'))
for c,i in enumerate(products.collect()):
    # print(c, i)
    df1 = input_json.select(
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
    df2 = df1.withColumn('id', f.col('id').cast(StringType())).\
                withColumn('order_date', f.to_timestamp('order_date')).\
                withColumn('product_price', f.col('product_price').cast(LongType())).\
                withColumn('shipping_cost', f.col('shipping_cost').cast(LongType())).\
                withColumn('customer_id', f.col('customer_id').cast(StringType())).\
                withColumn('quantity', f.col('quantity').cast(IntegerType()))
    #boolean condition
    df3 = df2.withColumn('is_email_marketing', f.when(f.col('is_email_marketing').isNotNull(), True).otherwise(False)).\
                    withColumn('is_sms_marketing', f.when(f.col('is_sms_marketing').isNotNull(), True).otherwise(False))

    #total (math operation)
    df4 = df3.withColumn('total_product_price', (f.col('product_price')*f.col('quantity'))).\
                withColumn('total_tax_product', f.col('total_tax_product_old').cast(LongType())).\
                drop('total_tax_product_old') # dorp karena spark itu ngisi value colomn nya urutan, buka sesuai nama

    #end transfrom

    df = df.union(df4)

df.show()