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
    dag_id='civil_aviation',
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

    ingest_data = BashOperator(
        task_id='ingest_data',
        bash_command=(
            '/usr/bin/sh '
            '/home/hadoop/scripts/civil_aviation/ingest_civil_aviation.sh '
        ),
    )

    with TaskGroup(
        group_id="transform_task_group",
        tooltip="Transform",
    ) as transform_task_group:
        transform_civil_aviation_flight = BashOperator(
            task_id='transform_civil_aviation_flight',
            bash_command=(
                "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit "
                "--files /home/hadoop/hive/conf/hive-site.xml "
                "/home/hadoop/scripts/civil_aviation/"
                "transform_civil_aviation_flights.py "
            )
        )

        transform_civil_aviation_details = BashOperator(
            task_id='transform_civil_aviation_details',
            bash_command=(
                "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit "
                "--files /home/hadoop/hive/conf/hive-site.xml "
                "/home/hadoop/scripts/civil_aviation/"
                "transform_civil_aviation_details.py "
            )
        )

    finish_process = DummyOperator(
        task_id='finish_process',
    )

    start_process >> \
        ingest_data >> \
        transform_task_group >> \
        finish_process


if __name__ == "__main__":
    dag.cli()
