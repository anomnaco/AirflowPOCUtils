from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
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
    start_date=days_ago(0),
    tags=['tutorial'],
    catchup=False
) as dag:

    bash_config_setup = BashOperator(
        task_id='bash_config_setup',
        bash_command='docker cp /home/anant/Desktop/obi/AirflowPOC/AirflowPOCUtils/config kronoscleanup_dse1_1:/app; docker cp /home/anant/Desktop/obi/AirflowPOC/AirflowPOCUtils/config kronoscleanup_dse2_1:/app',
        dag=dag
    )

    bash_vacuuming_run = BashOperator(
        task_id='bash_vacuuming_run',
        bash_command='docker exec kronoscleanup_dse1_1 /app/config/cleanUp.sh localhost /app/config config.yaml 2020-01-01 60 ',
        dag=dag
    )

    spark_sql_pre_count = BashOperator(
        task_id = "spark_sql_pre_count",
        bash_command="docker exec kronoscleanup_dse1_1 /app/config/getCount.sh ",
        dag=dag
    )

    bash_config_setup_run_two = BashOperator(
        task_id='bash_config_setup_run_two',
        bash_command='docker cp /home/anant/Desktop/obi/AirflowPOC/AirflowPOCUtils/config/config_2.yaml kronoscleanup_dse1_1:/app/config/config.yaml; docker cp /home/anant/Desktop/obi/AirflowPOC/AirflowPOCUtils/config/config_2.yaml kronoscleanup_dse2_1:/app/config/config.yaml ',
        dag=dag
    )

    bash_vacuuming_run_two = BashOperator(
        task_id='bash_vacuuming_run_two',
        bash_command='docker exec kronoscleanup_dse1_1 /app/config/cleanUp.sh localhost /app/config config.yaml 2020-01-01 60 ',
        dag=dag
    )

    spark_sql_post_count = BashOperator(
        task_id = "spark_sql_post_count",
        bash_command="docker exec kronoscleanup_dse1_1 /app/config/getCount.sh ",
        dag=dag
    )

    bash_config_setup >> bash_vacuuming_run >> bash_config_setup_run_two >> spark_sql_pre_count >> bash_vacuuming_run_two >> spark_sql_post_count
