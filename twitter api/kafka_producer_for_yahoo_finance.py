import requests
import time
from bs4 import BeautifulSoup
from kafka import SimpleProducer, SimpleClient


# template = '<span class="Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)">1,1441</span>'
# url = 'https://fr.finance.yahoo.com/quote/EURUSD=X?p=EURUSD=X'
#
#
# class APICaller:
#     def __init__(self, api_key='1IW8W6I7T3PYY0LM'):
#         self.api_key = api_key
#         self.base_URL = 'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE' + \
#                         '&from_currency={}&to_currency={}&apikey={}'
#
#     def request(self, from_currency='EUR', to_currency='USD', verbose=False):
#
#         url = self.base_URL.format(from_currency, to_currency, self.api_key)
#         response = requests.get(url=url)
#         if verbose:
#             print(response)
#
#         return json.loads(response.text)
#
#     def request_continuous(self, interval=2, count=10, from_currency='EUR', to_currency='USD',  stdout=True):
#
#         values = []
#         for t in range(count):
#             time.sleep(interval)
#             request_response = self.request(from_currency=from_currency, to_currency=to_currency, verbose=False)
#             values.append(request_response)
#             if stdout:
#                 print(request_response)
#         return values


class APICallerYahoo:
    """
    fake api caller
    """
    def __init__(self):
        self.base_URL = 'https://fr.finance.yahoo.com/quote/EURUSD=X?p=EURUSD=X'

    def request(self, verbose=False):

        response = requests.get(self.base_URL)
        if verbose:
            print(response)
        soup = BeautifulSoup(response.text, features='html.parser')
        value = next(soup.find('span', attrs={'data-reactid': '14'}).children)
        producer.send_messages('finance', str(value).encode('utf-8'))
        return value

    def request_continuous(self, interval=5, count=10, stdout=True):

        values = []
        for t in range(count):
            time.sleep(interval)
            request_response = self.request(verbose=False)
            values.append(request_response)
            if stdout:
                print(request_response)
        return values


if __name__ == "__main__":
    kafka_client = SimpleClient("localhost:9092")
    producer = SimpleProducer(kafka_client)
    api_caller = APICallerYahoo()
    api_caller.request_continuous(interval=2, count=10)

