from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import IDF, HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
from nltk.corpus import stopwords
import re
import json


# directory_path = 'file:///home/hduser/twitter_data'
# file_path = '/data/twitter_data/z_sample.csv'
file_path = '/home/paul/sentiment analysis/z_sample.csv'


def tokenize(text, stop_words, common_words):
    # defining regular expressions
    word_re = '\\w+'
    pseudo_re = '\\@[a-zA-Z0-9_]*'
    link_re = 'http(s)?://[a-zA-Z0-9./\\-]*'

    # stemming = SnowballStemmer(language='english')

    # replacing pseudos
    text1 = re.sub(pattern=pseudo_re, string=text, repl='pseudotwitterreplacement')
    # replacing links
    text1 = re.sub(pattern=link_re, string=text1, repl='linkwebsitereplacement')
    # replacing RTs
    text1 = re.sub(pattern='RT', string=text1, repl='retweetreplacement')

    # finding tokens
    tokens = [t for t in re.findall(pattern=word_re, string=text1.lower()) if t in common_words]

    # stemming tokens
    # tokens = [stemming.stem(t) for t in tokens if stemming.stem(t) not in stop_words]

    return tokens


def load_stopwords():
    try:
        sw = set(stopwords.words('english'))
    except LookupError:
        import nltk
        nltk.download('stopwords')
    finally:
        sw = set(stopwords.words('english'))
        # we want to keep some of the words
        words_to_keep = {'no', 'not', 'up', 'off', 'down', 'yes'}
        sw = sw.difference(words_to_keep)

    return sw


def load_common_words(directory='.'):
    cm = set(open('{}/most_common_us_words.txt'.format(directory)).read().split('\n'))
    cm.update(['pseudotwitterreplacement', 'linkwebsitereplacement', 'retweetreplacement'])
    return cm


def create_hash_table(common_words, stop_words):
    words = common_words.difference(stop_words)
    return {w: i for i, w in enumerate(words)}


def compute_tf(tokens, reference_table):
    hash_table = {}
    for token in tokens:
        if token in reference_table.keys():
            hash_table[reference_table[token]] = hash_table.get(reference_table[token], 0) + 1
    sparse_vector = SparseVector(len(reference_table), hash_table)
    return sparse_vector


if __name__ == '__main__':

    # create a spark context
    spark_context = SparkContext.getOrCreate()
    sql_context = SQLContext(sparkContext=spark_context)

    # defining the schema of the data
    schema = StructType([
        StructField('label', IntegerType(), True),
        StructField('id', StringType(), True),
        StructField('date', StringType(), True),
        StructField('query', StringType(), True),
        StructField('user', StringType(), True),
        StructField('text', StringType(), True)
    ])
    useless_columns = ['id', 'date', 'query', 'user']

    # load data
    df = sql_context.read.csv(path=file_path, schema=schema)
    for col in useless_columns:
        df = df.drop(col)

    # using a rdd
    rdd = df.rdd.map(Row.asDict)

    # pprint.pprint(rdd.take(2))

    # getting the stop words
    sw = load_stopwords()

    cm = load_common_words()

    reference_table = create_hash_table(common_words=cm, stop_words=sw)

    # tokenizing the text
    rdd = rdd.map(lambda d:
                  {
                      'tokens': tokenize(text=d['text'], stop_words=sw, common_words=cm),
                      'label': d['label']
                  }).\
        map(lambda d: LabeledPoint(0 if d['label'] == 0 else 1,
                                   compute_tf(tokens=d['tokens'],
                                              reference_table=reference_table)))


    logistic_regression = LogisticRegressionWithSGD()
    trained_logistic_regression = logistic_regression.train(data=rdd)

    trained_parameters = {
        'weights': trained_logistic_regression.weights.toArray().tolist(),
        'intercept': trained_logistic_regression.intercept
    }

    with open('model.json', 'w') as model_file:
        json.dump(trained_parameters, fp=model_file)
