from bs4 import BeautifulSoup
import requests

base_url = 'http://masterrussian.com/vocabulary/'
terminology = 'most_common_words'
terminations = ['_' + str(i) + '.htm' for i in range(1, 13)]
terminations[0] = '.htm'

if __name__ == '__main__':
    data = []
    for i, t in enumerate(terminations):

        r = requests.get(base_url + terminology + t)

        soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
        if i > 0:
            vocabulary_table = soup.find_all('table')[1]
        else:
            vocabulary_table = soup.find_all('table')[-2]

        for line in vocabulary_table.find_all('tr')[1:]:

            line_content = line.find_all('td')
            data.append([cell.text for cell in line_content])

    str_data ='\n'.join([';'.join(line) for line in data])

    with open('/home/paul/Desktop/russian_vocabulary.csv', 'w', encoding='utf-8') as file:

        file.write(str_data)

    print(str_data)
