import datetime
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'

default_args = {
    "owner": "snowflakedatapipelinepro",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "customer_orders_datapipeline_dynamic_batch_id",
    default_args=default_args,
    description="Runs data pipeline",
    schedule_interval=None,
    is_paused_upon_creation=False,
)

bash_task = BashOperator(task_id="run_bash_echo", bash_command="echo 1", dag=dag)

post_task = BashOperator(task_id="post_dbt", bash_command="echo 0", dag=dag)

batch_id =str(datetime.datetime.now().strftime("%Y%m%d%H%M"))
print("BATCH_ID = " + batch_id)

task_customer_landing_to_processing = BashOperator(
 task_id="customer_landing_to_processing",
 bash_command='aws s3 mv s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/customers/landing/ s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/customers/processing/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
task_customers_processing_to_processed = BashOperator(
 task_id="customer_processing_to_processed",
 bash_command='aws s3 mv s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/customers/processing/{0}/ s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/customers/processed/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
task_orders_landing_to_processing = BashOperator(
 task_id="orders_landing_to_processing",
 bash_command='aws s3 mv s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/orders/landing/ s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/orders/processing/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
task_orders_processing_to_processed = BashOperator(
 task_id="orders_processing_to_processed",
 bash_command='aws s3 mv s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/orders/processing/{0}/ s3://s3-ecommerce-snowflake-airflow-project-qh/firehose/orders/processed/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
snowflake_query_orders = [
    """
    COPY INTO "RETAIL_DB"."RETAIL_SCHEMA"."ORDERS_RAW"(
        O_BATCH_ID,
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERSTATUS,
        O_TOTALPRICE,
        O_ORDERDATE,
        O_ORDERPRIORITY,
        O_CLERK,
        O_SHIPPRIORITY,
        O_COMMENT
    ) FROM ( 
        SELECT '{0}',t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9 
        FROM @ORDERS_RAW_STAGE t);
    """.format(batch_id),
]

snowflake_show_tables = ["""SHOW TABLES;"""]

snowflake_query_customers = [
    """
    COPY INTO "RETAIL_DB"."RETAIL_SCHEMA"."CUSTOMERS_RAW"(
        C_BATCH_ID,
        C_CUSTKEY, 
        C_NAME,
        C_ADDRESS,
        C_NATIONKEY,
        C_PHONE,
        C_ACCTBAL,
        C_MKTSEGMENT,
        C_COMMENT
    ) FROM ( 
        SELECT '{0}',t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8 
        FROM @CUSTOMER_RAW_STAGE t);
    """.format(batch_id),
]


snowflake_query_customer_orders_small_transformation = [
    """
    INSERT INTO "RETAIL_DB"."RETAIL_SCHEMA"."ORDER_CUSTOMER_DATE_PRICE" (
        CUSTOMER_NAME,
        ORDER_DATE,
        ORDER_TOTAL_PRICE,
        BATCH_ID)
    SELECT 
        c.C_NAME as customer_name,
        o.O_ORDERDATE as order_date,
        sum(o.O_TOTALPROCE) as order_total_price,
        c.C_BATCH_ID
    FROM orders_raw o 
    JOIN customers_raw c 
    ON o.O_CUSTKEY = c.C_CUSTKEY 
        AND o.O_BATCH_ID = c.C_BATCH_ID
    WHERE O_ORDERSTATUS= 'F'
    GROUP BY C_NAME,O_ORDERDATE, c.C_BATCH_ID
    ORDER BY O_ORDERDATE;
    """,
]

snowflake_show_tables = SnowflakeOperator(
    task_id='snowflake_show_tables',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=snowflake_show_tables,
    warehouse="RETAIL_WH_XS",
    database="RETAIL_DB",
    schema="RETAIL_SCHEMA",
    role="RETAIL_ROLE",
)


snowflake_orders_sql_str = SnowflakeOperator(
    task_id='snowflake_raw_insert_order',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=snowflake_query_orders,
    warehouse="RETAIL_WH_XS",
    database="RETAIL_DB",
    schema="RETAIL_SCHEMA",
    role="RETAIL_ROLE",
)

snowflake_customers_sql_str = SnowflakeOperator(
    task_id='snowflake_raw_insert_customers',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=snowflake_query_customers,
    warehouse="RETAIL_WH_XS",
    database="RETAIL_DB",
    schema="RETAIL_SCHEMA",
    role="RETAIL_ROLE",
)

snowflake_order_customers_small_transformation = SnowflakeOperator(
    task_id='snowflake_order_customers_small_transformation',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=snowflake_query_customer_orders_small_transformation,
    warehouse="RETAIL_WH_XS",
    database="RETAIL_DB",
    schema="RETAIL_SCHEMA",
    role="RETAIL_ROLE",
)

[task_orders_landing_to_processing >> snowflake_show_tables >> snowflake_orders_sql_str >> task_orders_processing_to_processed, task_customer_landing_to_processing >> snowflake_customers_sql_str >> task_customers_processing_to_processed] >> snowflake_order_customers_small_transformation >> post_task