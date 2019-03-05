import requests

url = 'http://localhost:8888'

print(requests.get(url=url).text)
