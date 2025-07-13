import json
import boto3
import os
import requests

OMDB_API_KEY = os.environ['OMDB_API_KEY']
S3_BUCKET = os.environ['S3_BUCKET']

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        movie = json.loads(record['body'])
        imdb_id = movie['id']
        print(imdb_id)

        response = requests.get(f"https://www.omdbapi.com/?apikey={OMDB_API_KEY}&i={imdb_id}")
        movie['omdb'] = response.json()

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{imdb_id}.json",
            Body=json.dumps(movie),
            ContentType='application/json'
        )

    return {"statusCode": 200, "body": "Movie enriched and saved"}
