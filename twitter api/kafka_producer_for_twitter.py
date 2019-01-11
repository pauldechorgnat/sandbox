from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy.streaming import Stream
from kafka import SimpleProducer, SimpleClient

consumer_key = "mwBuTAg4KDTBZYyIXyp46Sc5n"
consumer_secret = "JJ9JLJvtubtBCx22GAhgQ2TgIZCnmXebCLQTYRe1Blo6tneMBz"
access_token = "2837218857-xwXthz3we19aYeYp645GYwLyaYMkChkLFuJJ5kM"
access_token_secret = "DdW9AbrW3WqwdCHADSsSK4ajEdviPJbsk65dm1HE0CuCj"


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
