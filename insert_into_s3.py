import boto3
from botocore.exceptions import NoCredentialsError
import json
import pandas as pd
from kafka.admin import KafkaAdminClient, NewTopic

with open('.env.json') as e:
    env = json.load(e)

# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id=env['aws_access_key_id'],
    aws_secret_access_key=env['aws_secret_access_key'],
    region_name=env['region_name']
)
    
# Creating the high level object oriented interface
resource = boto3.resource(
    's3',
    aws_access_key_id=env['aws_access_key_id'],
    aws_secret_access_key=env['aws_secret_access_key'],
    region_name=env['region_name']
)

# Fetch the list of existing buckets
clientResponse = client.list_buckets()

def upload_file(local_file: str, bucket_name: str, s3_file_name: str):
    """
    Function to upload a file to an S3 bucket
    """

    s3_client = boto3.client('s3')
    try:
        print('files is being uploaded')
        s3_client.upload_file(local_file, bucket_name, s3_file_name)
        print('file successfully uploaded')
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def list_files(bucket: str):
    """
    Function to list files in a given S3 bucket
    """
    s3 = boto3.client('s3')
    contents = []
    for item in s3.list_objects(Bucket=bucket)['Contents']:
        contents.append(item)
    print(contents)
    return contents

def download_file(file_name: str, bucket: str):
    """
    Function to download a given file from an S3 bucket
    """
    s3 = boto3.resource('s3')
    output = f"downloads/{file_name}"
    s3.Bucket(bucket).download_file(file_name, output)

    return output

def read_transcription_files(bucket_name: str, filename: str):
    """ Reading the individual files from the AWS S3 buckets and putting them in dataframes """
    import io  # Python 3.x
    
    obj = resource.Object(bucket_name, filename)
    data=obj.get()['Body'].read()
    df = pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False)
    # print(df.head(10))

    return df

def create_topic(topic_name: str):
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9092' )

    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=2))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    print(admin_client.list_topics())

if __name__ == "__main__":
    # Print the bucket names one by one
    # print('Printing bucket names...')
    # for bucket in clientResponse['Buckets']:
    #     print(f'Bucket Name: {bucket["Name"]}')

    # upload_file('Clean_Amharic.txt', 'tutors-kafka', 'transcriptions/Amharic_transcriptions/Clean_Amharic.txt')
    # upload_file('Clean_Amharic.txt', '10ac-batch-4', 'transcriptions/Amharic_transcriptions/Clean_Amharic.txt')
    # list_files('tutors-kafka')
    # read_transcription_files()
    upload_file('myFile.csv', 'tutors-kafka', 'transcriptions/Amharic_transcriptions/myFile.csv')
    