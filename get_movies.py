
import pandas as pd
import datetime
import boto3

SQS_QUEUE_URL = 'test'

def get_movies():
    #I Had issues getting this data from this s3 bucket provided. I downloaded from the internet and saved it as a csv file in the repo and will use that instead.
    # response = requests.get('https://top-movies.s3.eu-central 1.amazonaws.com/Top250Movies.json')
    # movies = response.json()
    movies = pd.read_csv('top250.csv')
    print(SQS_QUEUE_URL)
    top_10_movies = movies.head(10)
    print(top_10_movies)
    # sqs = boto3.client('sqs')
    for movie in top_10_movies:
        print('Sending message to queue')
        print('Movie: ', movie)
        # sqs.send_message(
        #     QueueUrl=QUEUE_URL,
        #     MessageBody=json.dumps(movie)
        # )


    


def main():
    start_time = datetime.datetime.now()
    print('Calling get_movies() function at time: ', start_time)
    get_movies()
    print('Finished processing with total time:' + str(datetime.datetime.now() - start_time))


main()
