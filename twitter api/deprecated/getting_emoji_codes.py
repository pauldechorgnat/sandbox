import requests
from bs4 import BeautifulSoup

if __name__ == '__main__':

    url = 'https://apps.timwhitlock.info/emoji/tables/unicode'

    r = requests.get(url=url)

    print('response status')
    print(r)

    soup = BeautifulSoup(r.text, 'html.parser')
    tables = soup.find_all('table')

    main_emoji_table = tables[0]

    rows = main_emoji_table.find_all('tr')

    data = []

    print(rows[1])

    for index, row in enumerate(rows):
        if index==0:
            continue
        else:
            cases = row.find_all('td')
            data.append(next(cases[7].children))

    with open('emoji.txt', 'w') as file:
        file.write('\n'.join(data))

    print(data)

