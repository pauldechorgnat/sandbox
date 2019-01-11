from pyspark import SparkContext
from pyspark.ml import PipelineModel
from pyspark.sql import SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from nltk.corpus import stopwords


if __name__ == '__main__':

    sc = SparkContext.getOrCreate()
    pipeline = PipelineModel.load('hdfs:///model_lr')

    file_path = '/data/twitter_data/z_sample.csv'

    # create a spark context
    sc = SparkContext.getOrCreate()

    # create a sql spark context
    sql = SQLContext(sc)

    # defining a schema for the data
    schema = StructType([
        StructField('polarity', IntegerType(), True),
        StructField('id', StringType(), True),
        StructField('date', StringType(), True),
        StructField('query', StringType(), True),
        StructField('user', StringType(), True),
        StructField('text', StringType(), True)
    ])
    useless_columns = ['id', 'date', 'query', 'user']

    # reading the data as a DataFrame and turning it into a RDD
    df = sql.read.csv(file_path,
                      schema=schema,
                      header=False,
                      sep=',')

    # dropping useless columns
    for col in useless_columns:
        df = df.drop(col)

    # splitting the data
    (df_train, df_val, df_test) = df.randomSplit(weights=[.6, .2, .2], seed=42)

    # defining regular expressions
    # word_re = '\\w+'
    # address_re = '\\@[a-zA-Z0-9_]*'
    # link_re = 'http(s)?://[a-zA-Z0-9./\\-]*'

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

    results = pipeline.transform(df_test)
    results.show(10)