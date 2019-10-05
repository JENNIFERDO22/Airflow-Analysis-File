import matplotlib.pyplot as plt
from utilities import preprocessing, upload_result_to_s3, get_path

def num_of_survivors(s3, bucket_name, **context):
    titanic_df = context['task_instance'].xcom_pull(task_ids='read_csv')
    df = preprocessing(titanic_df, 'Survived', -1)
    df = df[['Survived', 'PassengerId']]
    num_of_survivors = df.groupby('Survived').count()

    print(num_of_survivors)

    filename = 'num_of_survivors'
    local_path = '/home/jennie/workspace/titanic/'

    dest_s3_csv, local_path_csv, dest_s3_img, local_path_img = get_path(num_of_survivors, filename, local_path)

    num_of_survivors.plot.bar()
    plt.xlabel('Survivor and Victims')
    plt.ylabel('Number of people')
    plt.title('Number of survivors (1) and victims (0)')
    plt.savefig(local_path_img)

    upload_result_to_s3(s3, bucket_name, dest_s3_csv, local_path_csv, dest_s3_img, local_path_img)