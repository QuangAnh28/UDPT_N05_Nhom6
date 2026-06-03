from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def print_hello():
    print("Xin chào! Đây là demo Airflow của Nhóm 6.")
    time.sleep(3)
    print("Tác vụ đã hoàn thành!")

default_args = {
    'owner': 'nhom6',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='demo_udpt_nhom6',
    default_args=default_args,
    description='Một DAG demo cơ bản',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',  # Đã sửa lỗi: Dùng 'schedule' thay cho 'schedule_interval'
    catchup=False,
) as dag:

    task_1 = BashOperator(
        task_id='in_ra_thong_diep',
        bash_command='echo "Bắt đầu chạy luồng công việc..."'
    )

    task_2 = PythonOperator(
        task_id='chay_ham_python',
        python_callable=print_hello
    )

    task_1 >> task_2