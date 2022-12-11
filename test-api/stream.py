import tweepy
from dotenv import load_dotenv
import os
import datetime
import pandas as pd

"""
Streming APIv2 Restrictions (Elevated access)
- 25 rules per stream
- 50 requests per 15 minutes when using the POST /2/tweets/search/stream/rules endpoint to add rules
- 50 Tweets/second delivery cap for connections
- Can only use the core operators when building your rule
- Can build rules up to 512 characters in length
- Cannot use the recovery and redundancy features
"""

# load tokens
load_dotenv()
CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')
BEARER_TOKEN = os.getenv('BEARER_TOKEN')


class MyStreamingClient(tweepy.StreamingClient):

    def init_stream(self):
        self.max_cnt, self.cnt = 10, 0
        self.df = pd.DataFrame()

    def on_tweet(self, tweet):
        print(tweet.data, '\n====\n')
        # self.df.append(tweet.data, ignore_index=True)
        self.df = pd.concat([self.df, pd.DataFrame(tweet.data)], ignore_index=True)

        self.cnt +=1
        if self.cnt >= self.max_cnt:
            print('Disconnect: ', self.cnt)
            self.disconnect()

    def on_closed(self, response):
        print('Connection closed (', datetime.datetime.now(), ').')


def main():

    streaming_client = MyStreamingClient(BEARER_TOKEN)
    streaming_client.init_stream()

    query = "くまちゃん OR ネコちゃん OR ワンちゃん -is:retweet"

    streaming_client.add_rules(tweepy.StreamRule(query))

    print('===start (', datetime.datetime.now(), ') ===')
    streaming_client.filter(tweet_fields=['created_at', 'author_id'])

    print(streaming_client.df)
    streaming_client.df.to_parquet("./df.parquet")
    
if __name__ == '__main__':
    main()