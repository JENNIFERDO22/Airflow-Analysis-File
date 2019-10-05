# task 4: Pclass with the most survived ========================
import logging
import matplotlib.pyplot as plt
from utilities import preprocessing, upload_result_to_s3, get_path

def most_survived_pclass(s3, bucket_name, **context):
    titanic_df = context['task_instance'].xcom_pull(task_ids='read_csv')
    titanic_df = preprocessing(titanic_df, 'Pclass', -1)
    df = titanic_df[titanic_df['Survived'] == 1]
    # survived = survived[survived['Pclass'] != -1]

    counts = df['Pclass'].value_counts().rename_axis('Pclass').reset_index(name='counts')
    pclass_plot = counts.sort_values(by=['Pclass'])
    pclass_max = pclass_plot['counts'].max()
    most_survived_pclass = pclass_plot[pclass_plot['counts'] == pclass_max]

    filename = 'most_survivors_pclass'
    local_path= '/home/jennie/workspace/titanic/'

    dest_s3_csv, local_path_csv, dest_s3_img, local_path_img = get_path(most_survived_pclass, filename, local_path)

    plt.bar(pclass_plot['Pclass'], pclass_plot['counts'])
    # for i, v in enumerate(pclass_plot['counts']):
    #     plt.text(i + 10, v + 5, str(v), color='black', fontsize=20)
    plt.xlabel('Pclass')
    plt.ylabel('Number of survived')
    plt.title('Number of survived by Pclass')
    plt.savefig(local_path_img)

    logging.critical("PCLASS")
    logging.critical(pclass_plot.to_string())
    
    upload_result_to_s3(s3, bucket_name, dest_s3_csv, local_path_csv, dest_s3_img, local_path_img)


    # logging.critical("titanic df")                                                                                                                                                                 
    # logging.critical(titanic_df.to_string())


