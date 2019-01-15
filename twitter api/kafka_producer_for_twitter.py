from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy.streaming import Stream
from kafka import SimpleProducer, SimpleClient
import argparse
import pprint
import json


print('using keys')
open('./credentials.auth').read().split('\n')[:-1]

consumer_key, consumer_secret, access_token, access_token_secret = open('./credentials.auth').read().split('\n')[:-1]


class StdOutListener(StreamListener):
    def __init__(self, count=10000, verbose=False):
        StreamListener.__init__(self)
        self.max_count = count
        self.counter = 0
        self.verbose = verbose

    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        if self.verbose:
            print('=' * 80 + '\t' + str(self.counter+1) + '/' + str(self.max_count) + '\t' + '=' * 80)
            pprint.pprint(json.loads(data))
            print()
        self.counter += 1
        if self.counter == self.max_count:
            return False
        return True

    def on_error(self, status):
        print(status)
        return False


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Streaming tweets into kafka')
    parser.add_argument('--verbose', type=bool, default=False)
    parser.add_argument('--count', type=int, default=10000)
    args = parser.parse_args()
    # connecting to the kafka server
    kafka_client = SimpleClient("localhost:9092")
    # creating a producer to this server
    producer = SimpleProducer(kafka_client)
    # creating a standard output listener of tweets
    listener = StdOutListener(count=args.count, verbose=args.verbose)
    # connecting to the twitter api
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # streaming tweets from Trump
    stream = Stream(auth, listener)
    stream.filter(track=["trump"])
