from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
import pytz

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'timezone': 'Asia/seoul',
}

dag = DAG(
    'donpa_itme01_dag',
    default_args=default_args,
    schedule_interval='0 * * * *',  # 매 정각에 실행
    catchup=False,  # 과거 작업은 실행하지 않음
)

#  실행
mariadb_submit_task = BashOperator(
    task_id='mariadb_update',
    bash_command='/usr/local/spark/bin/spark-submit --master yarn --deploy-mode client --jars /usr/local/spark/jars/mariadb-java-client-2.7.0.jar /home/ubuntu/dataflow_hdfs_datamart/donpa_item1.py',
    dag=dag,
)


# Task 간 의존성 설정
# mariadb_submit_task >> mariadb_submin_tast1

