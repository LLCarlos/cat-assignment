import json
import boto3
import requests
import os


SQS_QUEUE_URL = 'test'

def lambda_handler(event, context):
    response = requests.get("https://top-movies.s3.eu-central-1.amazonaws.com/Top250Movies.json")
    movies = json.loads(response.text)

    top_10 = movies['items'][0:10]

    sqs = boto3.client('sqs')
    for movie in top_10:
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(movie)
        )
    return {"statusCode": 200, "body": "Movie enriched and saved"}


lambda_handler(None, None)