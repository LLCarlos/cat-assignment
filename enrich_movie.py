import json
import boto3
import os
import requests

OMDB_API_KEY = os.environ['OMDB_API_KEY']
S3_BUCKET = os.environ['S3_BUCKET_URL']
SQS_URL = os.environ['SQS_URL']

s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def lambda_handler(event, context):
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
    if 'Messages' in response:
        for message in response['Messages']:
            print(message)
            movie = json.loads(message['Body'])
            imdb_id = movie['id']

            response = requests.get(f"https://www.omdbapi.com/?apikey={OMDB_API_KEY}&i={imdb_id}")
            movie['omdb'] = response.json()

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"{imdb_id}.json",
                Body=json.dumps(movie),
                ContentType='application/json'
            )

            receipt_handle = message['ReceiptHandle']

            sqs.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=receipt_handle
            )

    return {"statusCode": 200, "body": "Movie enriched and saved"}
