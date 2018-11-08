from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import twitter_credentials as tc
import numpy as np
import pandas as pd


# Twitter clients

class TwitterClient():

    # Constructor
    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)

        self.twitter_user = twitter_user

    def get_twitter_client_api(self):
        return self.twitter_client



    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self, num_friends):
        friend_list = []
        for friend in Cursor(self.twitter_client.friends, id=self.twitter_user).items(num_friends):
            friend_list.append(friend)

        return friend_list

    def get_home_timeline_tweets(self, num_tweets):
        home_timeline_tweets = []
        for tweet in Cursor(self.twitter_client.home_timeline, id=self.twitter_user).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets


# Twitter Authenticator
class TwitterAuthenticator():
    def authenticate_twitter_app(self):
        auth = OAuthHandler(tc.consumer_key, tc.consumer_secret)
        auth.set_access_token(tc.access_token, tc.access_token_secret)
        return auth


class TwitterStreamer:
    # Class for streaming and processing live tweets

    # Constructor
    def __init__(self):
        self.twitter_authenticator = TwitterAuthenticator()

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):

        # This handles the Twitter authentication and the connection to the Twitter Streaming API

        listener = TwitterListener(fetched_tweets_filename)
        auth = self.twitter_authenticator.authenticate_twitter_app()

        stream = Stream(auth, listener)

        # This line filters the tweets we want
        stream.filter(track=hash_tag_list)


class TwitterListener(StreamListener):

    # This is the basis listener class that prints received tweets to stdout

    def __init__(self, fetched_tweets_filename):
        self.fetch_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetch_tweets_filename, 'a') as tf:
                tf.write(data)
                return True

        except BaseException as e:
            print("Error on data: %s" % str(e))

        return True

    def on_error(self, status):
        if status == 420:
            # Returning False if rate limit occurs
            return False
        print(status)


class TweetAnalyzer():

    # Functionality for analyzing and categorizing contents from tweets

    def tweets_to_data_frame(self, tweets):
        df = pd.DataFrame([tweet.text for tweet in tweets], columns=['Tweets'])
        #df['date'] = np.array([tweet.created_at for tweet in tweets])
        df['likes'] = np.array([tweet.favorite_count for tweet in tweets])
        #df['retweet'] = np.array([tweet.retweet_count for tweet in tweets])
        return df
    


# This is the main function
if __name__ == "__main__":

    twitter_client = TwitterClient()
    tweet_analyzer = TweetAnalyzer()
    api = twitter_client.get_twitter_client_api()

    tweets = api.user_timeline(screen_name='Microsoft', count=20)

    df = tweet_analyzer.tweets_to_data_frame(tweets)
    print(df.head(10))


