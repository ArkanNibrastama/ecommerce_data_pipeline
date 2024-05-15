import great_expectations as gx
from datetime import datetime

context = gx.get_context()

# get the csv file that contain in today's folder
datasource = context.sources.add_or_update_spark_filesystem(
    name="transformed_data",
    base_directory="../transformed_data/"+datetime.now().strftime('%Y%m%d')+"/"
)
datasource.add_csv_asset(name='asset',batching_regex=r'.*\.csv$',header=True)

# nanti waktu di GXOperator di airflow itu gausah pake parameter 'data_asset_name'
data_asset = context.get_datasource('transformed_data').get_asset('asset')
batch_req = data_asset.build_batch_request()

context.add_or_update_expectation_suite("my_expectation_suite")

validator = context.get_validator(
    batch_request=batch_req,
    expectation_suite_name="my_expectation_suite",
)

#define test
validator.expect_column_to_exist(column='id')
validator.expect_column_to_exist(column='order_date')
validator.expect_column_to_exist(column='payment_gateway')
validator.expect_column_to_exist(column='shipping_code')
validator.expect_column_to_exist(column='shipping_province')
validator.expect_column_to_exist(column='shipping_country')
validator.expect_column_to_exist(column='shipping_cost')
validator.expect_column_to_exist(column='customer_id')
validator.expect_column_to_exist(column='is_email_marketing')
validator.expect_column_to_exist(column='is_sms_marketing')
validator.expect_column_to_exist(column='customer_province')
validator.expect_column_to_exist(column='customer_country')
validator.expect_column_to_exist(column='product_name')
validator.expect_column_to_exist(column='product_variant')
validator.expect_column_to_exist(column='product_vendor')
validator.expect_column_to_exist(column='product_price')
validator.expect_column_to_exist(column='quantity')
validator.expect_column_to_exist(column='total_product_price')
validator.expect_column_to_exist(column='total_tax_product')

validator.expect_column_values_to_be_null(column='id')
validator.expect_column_values_to_be_null(column='order_date')
validator.expect_column_values_to_be_null(column='payment_gateway')
validator.expect_column_values_to_be_null(column='shipping_code')
validator.expect_column_values_to_be_null(column='shipping_province')
validator.expect_column_values_to_be_null(column='shipping_country')
validator.expect_column_values_to_be_null(column='shipping_cost')
validator.expect_column_values_to_be_null(column='customer_id')
validator.expect_column_values_to_be_null(column='is_email_marketing')
validator.expect_column_values_to_be_null(column='is_sms_marketing')
validator.expect_column_values_to_be_null(column='customer_province')
validator.expect_column_values_to_be_null(column='customer_country')
validator.expect_column_values_to_be_null(column='product_name')
validator.expect_column_values_to_be_null(column='product_variant')
validator.expect_column_values_to_be_null(column='product_vendor')
validator.expect_column_values_to_be_null(column='product_price')
validator.expect_column_values_to_be_null(column='quantity')
validator.expect_column_values_to_be_null(column='total_product_price')
validator.expect_column_values_to_be_null(column='total_tax_product')

validator.expect_column_values_to_be_of_type(column='id', type_='StringType')
validator.expect_column_values_to_be_of_type(column='order_date', type_='TimestampType')
validator.expect_column_values_to_be_of_type(column='payment_gateway', type_='StringType')
validator.expect_column_values_to_be_of_type(column='shipping_code', type_='StringType')
validator.expect_column_values_to_be_of_type(column='shipping_province', type_='StringType')
validator.expect_column_values_to_be_of_type(column='shipping_country', type_='StringType')
validator.expect_column_values_to_be_of_type(column='shipping_cost', type_='LongType')
validator.expect_column_values_to_be_of_type(column='customer_id', type_='StringType')
validator.expect_column_values_to_be_of_type(column='is_email_marketing', type_='BooleanType')
validator.expect_column_values_to_be_of_type(column='is_sms_marketing', type_='BooleanType')
validator.expect_column_values_to_be_of_type(column='customer_province', type_='StringType')
validator.expect_column_values_to_be_of_type(column='customer_country', type_='StringType')
validator.expect_column_values_to_be_of_type(column='product_name', type_='StringType')
validator.expect_column_values_to_be_of_type(column='product_variant', type_='StringType')
validator.expect_column_values_to_be_of_type(column='product_vendor', type_='StringType')
validator.expect_column_values_to_be_of_type(column='product_price', type_='LongType')
validator.expect_column_values_to_be_of_type(column='quantity', type_='IntegerType')
validator.expect_column_values_to_be_of_type(column='total_product_price', type_='LongType')
validator.expect_column_values_to_be_of_type(column='total_tax_product', type_='LongType')

# if wanna test the unit case, make a checkpoint and test it
checkpoint = context.add_or_update_checkpoint(
    name="my_checkpoint",
    validations=[
        {
            "batch_request": batch_req,
            "expectation_suite_name": "my_expectation_suite",
        },
    ],
)

checkpoint_result = checkpoint.run()

print(checkpoint_result)

# save great expectaitons suite (test case template)
validator.save_expectation_suite('gx/expectations/data_quality.json')