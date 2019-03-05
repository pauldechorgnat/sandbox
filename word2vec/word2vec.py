"""
https://towardsdatascience.com/learn-word2vec-by-implementing-it-in-tensorflow-45641adaf2ac
"""

import numpy as np
import tensorflow as tf
import wget
import os
import re
import pprint
from tqdm import tqdm


if __name__ == '__main__':
    # downloading data
    if 'pg1661.txt' not in os.listdir():
        wget.download('http://www.gutenberg.org/cache/epub/1661/pg1661.txt')

    # reading text
    raw_text = open('pg1661.txt', 'r').read().lower()

    # creating a regular expression for words
    regex_word = re.compile('\\w+')

    # finding the words
    words = regex_word.findall(raw_text)
    print('length of the text', len(words))

    # creating the vocabulary
    word_to_index = {word: index for index, word in enumerate(set(words))}
    index_to_word = {index: word for word, index in word_to_index.items()}
    vocabulary_size = len(word_to_index)
    print('length of the vocabulary:', vocabulary_size)

    # getting the sentences
    sentences = raw_text.split('.')
    print('number of sentences:', len(sentences))
    sentences = [regex_word.findall(sentence) for sentence in sentences]
    pprint.pprint(sentences[100:110])

    # defining the window size
    window_size = 2

    # defining data
    data = []
    for sentence in tqdm(sentences):
        for index, word in enumerate(sentence):
            data.append(
                (word_to_index[word],
                 [word_to_index[w] for w in sentence[max(index-window_size, 0):index + window_size + 1] if w != word])
            )

    def to_one_hot(word, null_vector=np.zeros(shape=vocabulary_size)):
        null_vector[word] = 1
        return null_vector

    # transforming data into one hot encoded vectors
    X = []
    y = []
    for word, context in tqdm(data):
        if len(context) == window_size*2:
            for w in context:
                X.append(to_one_hot(word))
                y.append(to_one_hot(w))
    print(len(X))
    print(len(y))

    input()


    # creating the network
    input_layer = tf.placeholder(tf.float32, shape=[None, vocabulary_size])
    true_label = tf.placeholder(tf.float32, shape=[None, vocabulary_size])

    embedding_dimension = 10

    W1 = tf.Variable(tf.random_normal([vocabulary_size, embedding_dimension]))
    b1 = tf.Variable(tf.random_normal([embedding_dimension]))
    hidden_representation = tf.add(tf.matmul(input_layer, W1), b1)

    W2 = tf.Variable(tf.random_normal([embedding_dimension, vocabulary_size]))
    b2 = tf.Variable(tf.random_normal([vocabulary_size]))
    prediction = tf.add(tf.matmul(hidden_representation, W2), b2)

    session = tf.Session()

    init = tf.global_variables_initializer()

    session.run(init)

    cross_entropy_loss = tf.reduce_mean(-tf.reduce_sum(true_label * tf.log(prediction), reduction_indices=[1]))

    train_step = tf.train.GradientDescentOptimizer(0.1).minimize(cross_entropy_loss)

    n_epochs = 1000
    batch_size = 32
    for e in range(n_epochs):
        losses = []
        for i in range(len(X)//batch_size):
            loss, _ = session.run([cross_entropy_loss, train_step],
                                  feed_dict={input_layer: X[i*batch_size:(i+1)*batch_size],
                                             true_label: y[i*batch_size:(i+1)*batch_size]
                                             }
                                  )
            losses.append(loss)
        print('epoch', e, '- loss', np.mean(losses))

