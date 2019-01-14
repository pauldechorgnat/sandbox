from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy.streaming import Stream
from kafka import SimpleProducer, SimpleClient

print('using keys')
open('./credentials.auth').read().split('\n')[:-1]

consumer_key, consumer_secret, access_token, access_token_secret = open('./credentials.auth').read().split('\n')[:-1]


class StdOutListener(StreamListener):
    def __init__(self, counter=10000):
        StreamListener.__init__(self)
        self.counter = counter

    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        # print(data)
        self.counter -= 1
        if self.counter == 0:
            return False
        return True

    def on_error(self, status):
        print(status)
        return False


if __name__ == '__main__':
    # connecting to the kafka server
    kafka_client = SimpleClient("localhost:9092")
    # creating a producer to this server
    producer = SimpleProducer(kafka_client)
    # creating a standard output listener of tweets
    listener = StdOutListener()
    # connecting to the twitter api
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # streaming tweets from Trump
    stream = Stream(auth, listener)
    stream.filter(track="trump")
