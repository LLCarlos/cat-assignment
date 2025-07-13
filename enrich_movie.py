import json
import boto3
import os
import requests

# env variables
OMDB_API_KEY = os.environ['OMDB_API_KEY']
S3_BUCKET = os.environ['S3_BUCKET_URL']
SQS_URL = os.environ['SQS_URL']

# clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def lambda_handler(event, context):
    # get movies from sqs queue. For each movie, get omdb data and save to s3
    response = sqs.receive_message(
        QueueUrl=SQS_URL,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=10,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=20
    )
    # if no messages, exit
    if 'Messages' not in response:
        return {"statusCode": 404, "body": "No messages in queue"}
    if 'Messages' in response:
        for message in response['Messages']:
            #fetch the movie 
            movie = json.loads(message['Body'])
            # fetch the id
            imdb_id = movie['id']

            # get omdb data from their api
            response = requests.get(f"https://www.omdbapi.com/?apikey={OMDB_API_KEY}&i={imdb_id}")
            movie['omdb'] = response.json()
            # save to s3
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"{imdb_id}.json",
                Body=json.dumps(movie),
                ContentType='application/json'
            )

            receipt_handle = message['ReceiptHandle']
            # clear queue
            sqs.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=receipt_handle
            )
    # if all good, return 200
    return {"statusCode": 200, "body": "Movie enriched and saved"}
