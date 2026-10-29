from airflow.sdk import DAG
from pendulum import DateTime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from include.orders_utils import extract, load, notify


with DAG(
    dag_id="orders_sql_sep",
    start_date=DateTime(2026, 10, 23),
    schedule=None
):

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract
    )

    load_orders = PythonOperator(
            task_id="load_orders",
            python_callable=load
        )

    transform_orders = SQLExecuteQueryOperator(
                task_id="transform_orders",
                sql="sql/orders.sql",
                conn_id="postgres"
            )

    notify_orders = PythonOperator(
                    task_id="notify_orders",
                    python_callable=notify
                )

extract_orders >> load_orders >> transform_orders >> notify_orders