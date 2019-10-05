import logging
import matplotlib.pyplot as plt

def preprocessing(df, column, missing_value_alternative):
    df[column]= df[column].fillna(missing_value_alternative)
    df = df[df[column] != -1]
    return df

def simple_upload(s3, bucket_name, key, local_path):
    s3_obj = s3.Object(bucket_name=bucket_name, key=key)
    s3_obj.upload_file(Filename= local_path)

def upload_result_to_s3(s3, bucket_name, dest_s3_csv, local_path_csv, dest_s3_img, local_path_img):
    # upload csv files
    simple_upload(s3, bucket_name, dest_s3_csv, local_path_csv)
    simple_upload(s3, bucket_name, dest_s3_img, local_path_img)

def get_path(df, filename, local_path):
    dest_s3_csv = filename + '.csv'
    local_path_csv = local_path + dest_s3_csv

    dest_s3_img = filename + '.png'
    local_path_img = local_path + dest_s3_img

    df.to_csv(local_path_csv)
    return dest_s3_csv, local_path_csv, dest_s3_img, local_path_img
