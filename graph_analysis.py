import json
import pprint


if __name__ == '__main__':

    # path to the json file containing the tweets
    path = './twitter api/large_tweets3.json'

    with open(path, 'r') as file:
        data = json.load(file)

    pprint.pprint(data[0])

    users = {}

    for tweet in data:
        users[tweet['user']['id']] = \
            users.get(tweet['user']['id'], []) + \
            [user_mentions['id'] for user_mentions in tweet.get('entities', {}).get('user_mentions', [])]

    print('graph information')
    pprint.pprint(users)

    unique_users = []
    for user in users:
        unique_users.append(user)
        unique_users.extend(users[user])
    unique_users = set(unique_users)
    print('number of emitters')
    print(len(users))
    print('number of nodes')
    print(len(unique_users))
