import pandas as pd

# task 3: Read csv ======================
def read_csv_from_s3(key, bucket_name):
# obj = s3.Object(bucket_name=bucket_name, key=key).get()
    s3_file_path = 's3://' + bucket_name + "/" + key
    titanic_df = pd.read_csv(s3_file_path)
    return titanic_df


# titanic_df = read_csv_from_s3(key = s3_filename, bucket_name = bucket_name)

# logging.info(titanic_df)
# print(titanic_df)

