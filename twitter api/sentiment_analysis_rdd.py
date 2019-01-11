import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import IDF, HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.ml.classification import LogisticRegression
from nltk.stem import SnowballStemmer
from nltk.corpus import stopwords
import re
import pprint


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

    # df.show(5)

    # using a rdd
    rdd = df.rdd.map(Row.asDict)

    # pprint.pprint(rdd.take(2))

    # getting the stop words
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

    cm = set(open('twitter api/most_common_us_words.txt').read().split('\n'))
    cm.update(['pseudotwitterreplacement', 'linkwebsitereplacement', 'retweetreplacement'])
    # tokenizing the text
    rdd = rdd.map(lambda d:
                  {
                      'tokens': tokenize(text=d['text'], stop_words=sw, common_words=cm),
                      'label': d['label']
                  })
    # pprint.pprint(rdd.take(2))

    # Splitting features and labels
    rdd_text = rdd.map(lambda d: d['tokens'])
    rdd = rdd.map(lambda d: d['label']).map(int)

    print('computing tf')
    # computing TF
    tf = HashingTF().transform(rdd_text)

    # computing IDF
    idf = IDF(minDocFreq=100).fit(tf)
    tf_idf = idf.transform(tf).map(lambda vector: vector.toArray())

    print('merging data')
    # regrouping data
    rdd = rdd.\
        zip(tf_idf).\
        map(lambda point: LabeledPoint(point[0], point[1]))
    # map(lambda row: Row(label=row[0], features=row[1]))

    results = rdd.take(10)

    # lr = LogisticRegression()
    # lr.fit(rdd_final.toDF())
    #
    # # logistic_regression = LogisticRegressionWithSGD()
    # # logistic_regression.train(data=rdd_final, iterations=10)

