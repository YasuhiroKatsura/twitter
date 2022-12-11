# Twitter API 動作テスト用

import tweepy
from datetime import datetime,timezone
import pytz
from dotenv import load_dotenv
import os

# load tokens
load_dotenv()
CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')
BEARER_TOKEN = os.getenv('BEARER_TOKEN')


def main():

    client = tweepy.Client(
        consumer_key = CONSUMER_KEY,
        consumer_secret = CONSUMER_SECRET,
        access_token = ACCESS_TOKEN,
        access_token_secret = ACCESS_TOKEN_SECRET,
        bearer_token = BEARER_TOKEN
    )

    # query = 'くまちゃん ネコちゃん' # AND
    query = 'くまちゃん OR ネコちゃん' # OR
    num = 100

    tweets = client.search_recent_tweets(query=query, max_results=num, tweet_fields=['created_at'])

    for tweet in tweets.data:
        print(tweet.created_at, tweet.id, tweet.text, '\n=====\n')
    

if __name__ == '__main__':
    main()