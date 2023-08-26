from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
# from airflow.utils.dates import days_ago

from datetime import datetime, timedelta


args = {
    'owner': 'airflow',
}

# next at 2023-08-14 00:00:00
# then at 2023-08-15 00:00:00
# then at 2023-08-16 00:00:00
# then at 2023-08-17 00:00:00
# then at 2023-08-18 00:00:00


with DAG(
    dag_id='exercise_class_9',
    default_args=args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['edvai', 'data_engineer', 'airflow'],
    params={'example_key': 'example_value'},
) as dag:
    start_process = DummyOperator(
        task_id='start_process',
    )

    with TaskGroup(
        group_id="ingest_task_group",
        tooltip="Ingest",
    ) as ingest_task_group:
        ingest_clientes = BashOperator(
            task_id='ingest_clientes',
            bash_command=(
                '/usr/bin/sh '
                '/home/hadoop/scripts/ingest_na_clientes.sh '
            )
        )

        ingest_envios = BashOperator(
            task_id='ingest_envios',
            bash_command=(
                '/usr/bin/sh '
                '/home/hadoop/scripts/ingest_na_envios.sh '
            )
        )

        ingest_order_details = BashOperator(
            task_id='ingest_order_details',
            bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_na_od.sh ',
        )

    with TaskGroup(
        group_id="transform_task_group",
        tooltip="Transform",
    ) as transform_task_group:
        transform_products_sold = BashOperator(
            task_id='transform_products_sold',
            bash_command=(
                "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit "
                "--files /home/hadoop/hive/conf/hive-site.xml "
                "/home/hadoop/scripts/transform_na_prod_sold.py "
            )
        )

        transform_products_sent = BashOperator(
            task_id='transform_products_sent',
            bash_command=(
                "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit "
                "--files /home/hadoop/hive/conf/hive-site.xml "
                "/home/hadoop/scripts/transform_na_prod_sent.py "
            )
        )

    finish_process = DummyOperator(
        task_id='finish_process',
    )

    start_process >> \
        ingest_task_group >> \
        transform_task_group >> \
        finish_process


if __name__ == "__main__":
    dag.cli()
