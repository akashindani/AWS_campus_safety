import logging
import boto3
from botocore.exceptions import ClientError
import datetime


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_collection(client):
    #Create collection
    response = client.create_collection(
        CollectionId='suspects_new'
    )
    print(response)

def add_to_collection(client):
    # add to collection
    response = client.index_faces(
        CollectionId='suspects',
        Image={
            'S3Object': {
                'Bucket': 'whichface',
                'Name': 'img_01.jpg',
            }
        },
        ExternalImageId='01',
        DetectionAttributes=[
            "ALL", "DEFAULT"
        ],
        MaxFaces=1,
        QualityFilter='AUTO'
    )

    print(response)

def create_input_stream(kinesisvideo_client):
    # input stream


    kinesisvideo_response = kinesisvideo_client.create_stream(
        StreamName='Cam_input_2',
        DataRetentionInHours=5,
        Tags={
            'Name': 'kinesisvideotag'
        }
    )
    print(kinesisvideo_response)


def create_output_stream(kinesis_client):
    # Output stream


    kinesis_client_response = kinesis_client.create_stream(
        StreamName='output_2',
        ShardCount=5
    )
    print(kinesis_client_response)
    # arn:aws:kinesis:us-east-1:688082143575:stream/output

def create_stream_processor(client):
    create_stream_processor_response = client.create_stream_processor(
        Input={
            'KinesisVideoStream': {
                'Arn': 'arn:aws:kinesisvideo:us-east-1:688082143575:stream/Cam_input_2/1580020579487'
            }
        },
        Name='main_stream_processor_3',
        Output={
            'KinesisDataStream': {
                'Arn': 'arn:aws:kinesis:us-east-1:688082143575:stream/output_2'
            }
        },
        Settings={
            'FaceSearch': {
                'CollectionId': 'suspects_new',
                'FaceMatchThreshold': 60
            }
        },
        RoleArn='arn:aws:iam::688082143575:role/main_rekog_role'
    )

    print(create_stream_processor_response)

def start_stream_processor():
    response = client.start_stream_processor(
        Name='main_stream_processor_2'
    )
    print(response)

def create_consumer(kinesis_client):
    response = kinesis_client.register_stream_consumer(
        StreamARN='arn:aws:kinesis:us-east-1:688082143575:stream/output_2',
        ConsumerName='consume_data_stream'
    )
    print(response)

def create_event_source_mapping(lambda_client):
    response = lambda_client.create_event_source_mapping(
        EventSourceArn='arn:aws:kinesis:us-east-1:688082143575:stream/output_2',
        FunctionName='use_data_stream',
        Enabled=True,
        BatchSize=50,
        #MaximumBatchingWindowInSeconds=100,
        ParallelizationFactor=10,
        StartingPosition='LATEST',
        MaximumRecordAgeInSeconds=300,
        BisectBatchOnFunctionError=True,
        MaximumRetryAttempts=3
    )

def get_signalling_channel(kinesisvideo_client):
    response = kinesisvideo_client.get_signaling_channel_endpoint(
        ChannelARN='arn:aws:kinesisvideo:us-east-1:688082143575:channel/demo_channel/1580033028517',
        SingleMasterChannelEndpointConfiguration={
            'Protocols': [
                'WSS' ,'HTTPS'
            ],
            'Role': 'MASTER'
        }
    )

    print(response)

def get_input_data_endpoint(kinesisvideo_client):
    response = kinesisvideo_client.get_data_endpoint(
        StreamARN='arn:aws:kinesisvideo:us-east-1:688082143575:stream/Cam_input_2/1580020579487',
        APIName= 'GET_MEDIA'
    )

    print(response)
def delete_stream_processor(client,stream_processor_name):
    response = client.delete_stream_processor(
        Name=stream_processor_name
    )

def delete_collection(client,collection_name):
    response = client.delete_collection(
        CollectionId=collection_name
    )

#Clients

client = boto3.client('rekognition')
kinesis_client = boto3.client('kinesis')
lambda_client = boto3.client('lambda')
kinesisvideo_client = boto3.client('kinesisvideo')
file_name="img_01.jpg"
bucket='whichface'

'''
#Upload file to s3
ret_status=upload_file(file_name, bucket)
if ret_status==True:
    print("File uploaded")
else:
    print("Upload error")
'''

#create_collection(client)
#aws:rekognition:us-east-1:688082143575:collection/suspects_new
#FaceModelVersion': '4.0'

#add_to_collection(client)

#create_input_stream(kinesisvideo_client)
# 'StreamARN': 'arn:aws:kinesisvideo:us-east-1:688082143575:stream/Cam_input_2/1580020579487'

#create_output_stream(kinesis_client)
#arn:aws:kinesis:us-east-1:688082143575:stream/output_2

#create_stream_processor(client)
#arn:aws:rekognition:us-east-1:688082143575:streamprocessor/main_stream_processor_2

#start_stream_processor


#create_consumer(kinesis_client)
#arn:aws:kinesis:us-east-1:688082143575:stream/output_2/consumer/consume_data_stream:1580036078

#create_event_source_mapping(lambda_client)

#get_signalling_channel(kinesisvideo_client)
#get_input_data_endpoint(kinesisvideo_client)

#stream_processor_name='main_stream_processor'
#delete_stream_processor(client,stream_processor_name)

response = client.list_stream_processors(
    MaxResults=123
)
print(response)

delete_collection(client,'suspects_new')

