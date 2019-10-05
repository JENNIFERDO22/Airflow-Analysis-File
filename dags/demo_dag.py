# DAG instance
from airflow import DAG
from datetime import datetime, timedelta

# workflow: tan_gai (sequence of tasks - schedule daily)
# airflow: tan gai k can nghi

default_args = {
    'owner': 'Jennie',
    'start_date': datetime(2019, 1, 1),
    # 'end_date': datetime(2018, 12, 30),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'tan_gai',
    default_args=default_args,
    # description='A simple tutorial DAG',
    schedule_interval='@daily'
)
# ==============================================
# ==============================================

from airflow.operators import PythonOperator, DummyOperator
# task 1: dummy =====================
# 3 types:

# sensor: keep running until a certain criteria is met
# HdfsSensor: Waits for a file or folder to land in HDFS

# operator: trigger a certain action (eg. call a function)
# PythonOperator

# transfer: move data from one location to another
# S3ToRedshiftTransfer: load files from s3 to Redshift

dummy_task = DummyOperator(
    task_id = 'dummy_start',
    # dag container
    dag=dag
)

# task 2: Upload file =====================
di_bar = PythonOperator(
    task_id = 'di_bar',
    # function that is invoked
    python_callable= mua_may_quay_cuong,
    # function arguments
    op_kwargs={
        'bai_hat': 'Vinahouse'
    },
    dag=dag
)

def mua_may_quay_cuong(bai_hat):
    print('Dang quay bai', bai_hat)

# ===================================================
# return argument

noi_chuyen = PythonOperator(
    task_id = 'noi_chuyen',
    python_callable= noi_chuyen,
    # dag container
    dag=dag
)

# task 2: Upload file =====================
hanh_dong = PythonOperator(
    task_id = 'hanh_dong',
    # function that is invoked
    python_callable= hanh_dong,
    # function arguments
    provide_context=True,
    dag=dag
)

def noi_chuyen():
    return "tang hoa"

def hanh_dong(**context):
    # context: hoan canh
    lam_gi = context['task_instance'].xcom_pull(task_ids='noi_chuyen')
    print(lam_gi)


# =================================
# =================================
# dependencies
dummy_task >> di_bar

di_bar >> [noi_chuyen, ca_khia]
# di_bar.set_downstream([noi_chuyen, ca_khia])

[noi_chuyen, ca_khia] << di_bar
di_bar >> noi_chuyen >> hanh_dong