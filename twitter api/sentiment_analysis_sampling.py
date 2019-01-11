import os
from collections import Counter
import numpy as np


if __name__ == '__main__':

    # getting file path
    directory_path = '/home/paul/sentiment analysis'
    file_path = os.path.join(directory_path, os.listdir(directory_path)[0])

    # creating a reproducible sampling
    np.random.seed(42)
    keep = iter(np.random.binomial(n=1, p=.4, size=1600000))

    # instantiating a list for the data and a list for the target to check the balance
    data = []
    targets = []

    # reading the data
    with open(file_path, 'r', encoding='cp1252') as file:
        for line in file:
            if next(keep) == 1:
                data.append(line)
                targets.append(int(line[1]))

    # saving the data
    with open(os.path.join(directory_path, 'z_sample.csv'), 'w', encoding='utf-8') as file:
        file.write('\n'.join(data))

    print(Counter(targets))



