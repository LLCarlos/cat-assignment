import json
import boto3
import requests
import os


SQS_QUEUE_URL = 'test'

def lambda_handler(event, context):
    response = requests.get("https://top-movies.s3.eu-central-1.amazonaws.com/Top250Movies.json")
    movies = json.loads(response.text)
    # get top 10 movies in the items response. since its already sorted, we can just take the first 10
    top_10 = movies['items'][0:10]
    # client for sqs
    sqs = boto3.client('sqs')
    # loop to send the messages
    for movie in top_10:
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(movie)
        )
    return {"statusCode": 200, "body": "Movie enriched and saved"}


lambda_handler(None, None)