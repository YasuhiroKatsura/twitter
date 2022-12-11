import tweepy
import datetime
import pandas as pd


class MyStreamingClient(tweepy.StreamingClient):

    def init_stream(self, max_tweets):
        self.max_cnt = max_tweets
        self.cnt = 0
        self.df = pd.DataFrame()

    def on_tweet(self, tweet):
        self.df = pd.concat([self.df, pd.DataFrame(tweet.data)], ignore_index=True)

        self.cnt +=1
        if self.cnt >= self.max_cnt:
            self.disconnect()

    def on_closed(self, response):
        pass

def run(BEARER_TOKEN, query, max_tweets):

    streaming_client = MyStreamingClient(BEARER_TOKEN)
    streaming_client.init_stream(max_tweets)

    streaming_client.add_rules(tweepy.StreamRule(query))

    streaming_client.filter(tweet_fields=['created_at', 'author_id']) 

    return streaming_client.df