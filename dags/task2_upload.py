# task 2: Upload file =====================
# filename: path to the file in local computer
# key: name in s3
def upload_file_to_s3(s3, filename, key, bucket_name):
    print('Uploading to s3')
    s3.Bucket(bucket_name).upload_file(filename, key)

