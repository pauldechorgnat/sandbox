import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import GradientBoostingClassifier
import numpy as np

if __name__ == '__main__':
    # importer les packages nécessaires

    # lire le fichier "movies_coms.csv'
    df = pd.read_csv('/home/paul/Downloads/movies_comments.csv')

    # Afficher les deux premières lignes de df
    df.head(10)

    # Définition du dictionnaire :
    sunText = ""
    for words in df.Text:
        sunText += " " + words

    from sklearn.feature_extraction.text import CountVectorizer
    from nltk.tokenize.regexp import RegexpTokenizer

    tokenizer = RegexpTokenizer("[a-zA-Zé]{4,}")
    vectorizer = CountVectorizer()
    vectorizer.fit_transform(tokenizer.tokenize(sunText.lower()))


    # Vectorization des mots :
    def vect1(words):
        liste = []
        tokens = tokenizer.tokenize(words.lower())
        for word in tokens:
            liste.append(vectorizer.transform([word]).toarray())

        return np.asarray(liste)


    def convY(word_conv):
        X_t = []
        Y_t = []
        for i in range(3, len(word_conv) - 2):
            a1 = np.concatenate((word_conv[i - 2][0], word_conv[i - 1][0], word_conv[i + 1][0], word_conv[i + 2][0]))
            Y_t.append(a1)
            X_t.append(word_conv[i][0])

        return np.asarray(X_t), np.asarray(Y_t)


    X_true, y_true = convY(vect1(sunText))

    import tensorflow as tf

    epoch = 10000
    batch_size = 10


    # tf.reset_default_graph()

    def dataset_def(X_, y_, batch_size=32):
        dataset_1 = tf.data.Dataset.from_tensor_slices((X_, y_)).repeat(1).batch(batch_size)
        iterator = dataset_1.make_one_shot_iterator()
        data_X, data_y = iterator.get_next()
        # data_X = tf.reshape(tf.cast(data_X, tf.float32),(batch_size,X_true.shape[1]))
        # data_y = tf.reshape(tf.cast(data_y, tf.float32),(batch_size,y_true.shape[1]))
        return data_X


    X_1 = dataset_def(X_true, y_true)
    print("00.")
    sess = tf.Session()
    sess.run(tf.global_variables_initializer())
    print(sess.run(X_1))
