import boto3
import pandas as pd

from airflow.operators import PythonOperator, DummyOperator
from airflow import DAG
from datetime import datetime, timedelta

from task2_upload import *
from task3_read import *
from task4_pclass_most_survived import *
from task5_avg_fare_by_pclass import *
from task6_num_survivors import *

s3 = boto3.resource('s3')
s3_filename = 'train.csv'
local_filename = '/home/jennie/workspace/titanic/train.csv'
bucket_name = 'airflow-demo-09092019'

default_args = {
    'owner': 'Jennie',
    'start_date': datetime(2019, 1, 1),
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG('abbbbbbbbb_titanic_analysis', default_args=default_args, schedule_interval='@once')

# task 1: dummy =====================
dummy_task = DummyOperator(
    task_id = 'dummy_start',
    dag=dag
)

# task 2: Upload file =====================
upload_task = PythonOperator(
    task_id = 'upload_file_to_s3',
    python_callable= upload_file_to_s3,
    op_kwargs={
        's3': s3,
        'filename': local_filename,
        'key': s3_filename,
        'bucket_name': bucket_name
    },
    dag= dag
)

# task 3: Read csv ======================
read_task = PythonOperator(
    task_id= "read_csv",
    python_callable= read_csv_from_s3,
    op_kwargs={
        'key': s3_filename,
        'bucket_name': bucket_name
    },
    dag=dag
)

# titanic_df = read_csv_from_s3(s3_filename, bucket_name)

# task 4: Pclass with the most survived ========================
pclass_most_survived_task = PythonOperator(
    task_id= "most_survived_pclass",
    python_callable= most_survived_pclass,
    op_kwargs={
        'bucket_name': bucket_name,            
        's3': s3
    },
    provide_context=True,
    dag=dag
)

# task 5: Average fare by pclass
avg_fare_by_pclass_task = PythonOperator(
    task_id= "avg_fare_by_pclass",
    python_callable= avg_fare_by_pclass,
    op_kwargs={
        'bucket_name': bucket_name,
        's3': s3
    },
    provide_context=True,
    dag=dag
)

# task 6: NUmber of survivors
num_of_survivors_task = PythonOperator(
    task_id= "num_of_survivors",
    python_callable= num_of_survivors,
    op_kwargs={
        # 'titanic_df': titanic_df,
        'bucket_name': bucket_name,
        's3': s3
    },
    provide_context=True,
    dag=dag
)

# task 7: dummy =====================
end_task = DummyOperator(
    task_id = 'end',
    dag=dag
)

dummy_task >> upload_task >> read_task >> [pclass_most_survived_task, avg_fare_by_pclass_task, num_of_survivors_task] >> end_task
