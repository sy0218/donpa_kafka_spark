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
    'spark_aabata_sang_dag',
    default_args=default_args,
    schedule_interval='30 * * * *',  # 정각 마다 실행
    catchup=False,  # 과거 작업은 실행하지 않음
)

# hdfs에 데이터 저장
spark_submit_task = BashOperator(
    task_id='aabata_sang_load_csv',
    bash_command='/usr/local/spark/bin/spark-submit --master yarn --deploy-mode client /home/ubuntu/dataflow_aabata_hdfs/marge_sang_aabata.py',
    dag=dag,
)

# mariadb에 데이터 저장
push_maria_task = BashOperator(
    task_id='aabata_sang_push_mariadb',
    bash_command='/usr/local/spark/bin/spark-submit --master yarn --deploy-mode client --jars /usr/local/spark/jars/mariadb-java-client-2.7.0.jar /home/ubuntu/dataflow_aabata_hdfs_datamart/sang_aabata_save_rdb.py',  
    dag=dag,
)


# Task 간 의존성 설정
spark_submit_task >> push_maria_task
