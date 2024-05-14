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

validator.expect_column_values_to_not_be_null(
    column="id"
)

validator.expect_column_values_to_be_of_type(
    column="id",
    type_="StringType"
)

# if wanna test the unit case, make a checkpoint and test it

# save great expectaitons suite (test case template)
validator.save_expectation_suite('gx/expectations/data_quality.json')