import matplotlib.pyplot as plt
from utilities import preprocessing, upload_result_to_s3, get_path

def avg_fare_by_pclass(s3, bucket_name, **context):
    titanic_df = context['task_instance'].xcom_pull(task_ids='read_csv')
    df = preprocessing(titanic_df, 'Fare', -1)
    df = df[['Pclass', 'Fare']]
    avg_fare_by_pclass = df.groupby('Pclass').mean()
    print(avg_fare_by_pclass)

    filename = 'avg_fare_by_pclass'
    local_path = '/home/jennie/workspace/titanic/'

    dest_s3_csv, local_path_csv, dest_s3_img, local_path_img = get_path(avg_fare_by_pclass, filename, local_path)

    avg_fare_by_pclass.plot.bar()
    plt.xlabel('Average fare')
    plt.ylabel('Passenger class')
    plt.title('Average fare by pclass')
    plt.savefig(local_path_img)

    upload_result_to_s3(s3, bucket_name, dest_s3_csv, local_path_csv, dest_s3_img, local_path_img)