from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from datetime import timedelta


args = {
    'owner': 'airflow',
}

# next at 2023-08-14 00:00:00
# then at 2023-08-15 00:00:00
# then at 2023-08-16 00:00:00
# then at 2023-08-17 00:00:00
# then at 2023-08-18 00:00:00


with DAG(
    dag_id='exercise_class_7',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['edvai', 'data_engineer', 'airflow'],
    params={'example_key': 'example_value'},
) as dag:
    start_process = DummyOperator(
        task_id='start_process',
    )

    ingest_data = BashOperator(
        task_id='ingest_data',
        bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_2.sh ',
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=(
            "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit "
            "--files /home/hadoop/hive/conf/hive-site.xml "
            "/home/hadoop/scripts/transform_tripdata.py "
        )
    )

    finish_process = DummyOperator(
        task_id='finish_process',
    )

    start_process >> ingest_data >> transform_data >> finish_process


if __name__ == "__main__":
    dag.cli()
