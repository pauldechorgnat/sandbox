import tweepy

api_key = 'mwBuTAg4KDTBZYyIXyp46Sc5n'
api_secret_key = 'JJ9JLJvtubtBCx22GAhgQ2TgIZCnmXebCLQTYRe1Blo6tneMBz'
access_token = '2837218857-xwXthz3we19aYeYp645GYwLyaYMkChkLFuJJ5kM'
access_secret_token = 'DdW9AbrW3WqwdCHADSsSK4ajEdviPJbsk65dm1HE0CuCj'

# creating access authorisation
auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_secret_token)


# defining a custom stream listener
class CustomStreamListener(tweepy.StreamListener):

    def __init__(self, count=None,
                 parent=None):
        """
        creating a CustomStreamListener object
        ---
        count : if None the stream will never stop, if 1, sets the number of streamed tweets to count
        verbose : if 0, does not display anything, if 1 displays tweet texts as it comes,
                  if more display remaining count every verbose iterations
        """
        # instantiating the superclass
        super(CustomStreamListener, self).__init__(parent)
        # setting other attributes
        self.count = count
        self.counter = count

    def on_status(self, status):
        """
        stores tweets in the database
        """
        # handling display options
        self.counter -= 1

        if self.counter >= 0:
            print(status.author.screen_name, 'said')
            print(status.text)
            print('at', status.created_at)
            return True
        else:
            return False


    def on_error(self, status_code):
        """
        prints the error code if an error happens when the stream is running
        ---
        allows us not to kill the stream if an error happens
        """
        print('Encountered error with status code:', status_code)
        return True

    # instantiating the listener


if __name__ == '__main__':
    listener = CustomStreamListener(count=10)
    streamingAPI = tweepy.streaming.Stream(auth, listener)
    streamingAPI.filter(track=['trump'])


