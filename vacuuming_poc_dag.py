from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'vacuuming_sample',
    default_args=default_args,
    description='An example Cassandra DAG using cassandra hooks',
    schedule_interval=timedelta(minutes=30),
    start_date=days_ago(2),
    tags=['tutorial'],
) as dag:

    bash_config_setup = BashOperator(
        task_id='bash_config_setup',
        bash_command='docker cp /home/obi/Documents/UKG/AirflowPOC/AirflowPOCUtils/config kronoscleanup_dse1_1:/app; docker cp /home/obi/Documents/UKG/AirflowPOC/AirflowPOCUtils/config kronoscleanup_dse2_1:/app',
        dag=dag
    )

    bash_vacuuming_run = BashOperator(
        task_id='bash_vacuuming_run',
        bash_command='docker exec -it kronoscleanup_dse1_1 dse spark-submit --master dse://172.20.0.3 --class com.kronos.kpifrm.vacuuming.cassandra.CleanUp --conf spark.kronos.config=/app/config/config.yaml --jars /jars/kronos.cleanup-assembly-0.0.1.jar --files=/app/config/config.yaml /jars/kronos.cleanup-assembly-0.0.1.jar localhost cassandra cassandra 2020-01-01 60'
    )

    bash_config_setup >> bash_vacuuming_run