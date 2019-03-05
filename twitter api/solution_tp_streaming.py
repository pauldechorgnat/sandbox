from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.linalg import SparseVector
from pyspark.streaming.kafka import KafkaUtils
from nltk.corpus import stopwords
from pyspark.mllib.classification import StreamingLogisticRegressionWithSGD
import json
import re
import datetime
from happybase import Connection
import ast


def tokenize(text, common_words):
    """
    function used to tokenize tweets
    """
    word_re = '\\w+'
    pseudo_re = '\\@[a-zA-Z0-9_]*'
    link_re = 'http(s)?://[a-zA-Z0-9./\\-]*'

    # replacing pseudos
    text1 = re.sub(pattern=pseudo_re, string=text, repl='pseudotwitterreplacement')
    # replacing links
    text1 = re.sub(pattern=link_re, string=text1, repl='linkwebsitereplacement')
    # replacing RTs
    text1 = re.sub(pattern='RT', string=text1, repl='retweetreplacement')

    # finding tokens
    tokens = [t for t in re.findall(pattern=word_re, string=text1.lower()) if t in common_words]

    return tokens


def load_stopwords():
    """
    function used to load stop words
    """
    # trying to load the english stop words
    try:
        stopwords.words('english')
    except LookupError:
        # if an error occurs, we download the stopwords with nltk
        import nltk
        nltk.download('stopwords')
    finally:
        # creating a set
        sw = set(stopwords.words('english'))
        # we want to keep some of the words because they can have a lot of sense
        words_to_keep = {'no', 'not', 'up', 'off', 'down', 'yes'}
        # removing those words from the list of stop words
        sw = sw.difference(words_to_keep)
    return sw


def load_common_words(directory='.'):
    """
    function used to load most common words
    """
    # loading the most common words
    cm = set(open('{}/most_common_us_words.txt'.format(directory)).read().split('\n'))
    # adding our placeholders
    cm.update(['pseudotwitterreplacement', 'linkwebsitereplacement', 'retweetreplacement'])
    return cm


def create_hash_table(common_words, stop_words):
    """
    function used to create a hash table of the words we want to keep
    """
    # deleting the stop words
    words = common_words.difference(stop_words)
    return {w: i for i, w in enumerate(words)}


def compute_tf(tokens, reference_table):
    """function used to compute term frequency"""
    hash_table = {}
    # running through the tokens
    for token in tokens:
        # if the token is indeed among those we want to keep
        if token in reference_table.keys():
            # updating the frequency table
            hash_table[reference_table[token]] = hash_table.get(reference_table[token], 0) + 1
    # returning a Sparse vector object
    sparse_vector = SparseVector(len(reference_table), hash_table)
    return sparse_vector


def put_data_into_hbase(rdd):
    """
    functions to store data into hbase table
    """
    # collecting the results
    results = rdd.collect()
    # computing the exact time: this will serve as the row id
    date = str(datetime.datetime.now())[:19]
    # making connection to the right
    connection = Connection(host='localhost', port=9090, autoconnect=True)
    table = connection.table(name='base_tweets')
    #
    for data in results:
        if data[0] == 0:
            table.put(row=date, data={'tweet_count:neg': str(data[1])})
        else:
            table.put(row=date, data={'tweet_count:pos': str(data[1])})

    connection.close()


if __name__ == '__main__':
    # creating a SparkContext object
    sc = SparkContext.getOrCreate()
    # setting the log level to avoid printing logs in the console
    sc.setLogLevel("WARN")
    # creating a Spark Streaming Context
    ssc = StreamingContext(sparkContext=sc, batchDuration=10)

    # setting up a model
    lr = StreamingLogisticRegressionWithSGD()
    # loading the pre-trained parameters
    parameters = json.load(open('model.json', 'r'))
    # assigning the pre-trained parameters to the logistic regression
    lr.setInitialWeights(parameters['weights'])
    # loading stop words
    stop_words = load_stopwords()
    # loading common words
    common_words = load_common_words()
    # creating the reference table
    reference_table = create_hash_table(common_words=common_words, stop_words=stop_words)

    # opening the stream
    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,
                                                topics=['trump'],
                                                kafkaParams={"metadata.broker.list": 'localhost:9092'})

    # getting only the useful information
    dfs = kafkaStream.map(lambda stream: stream[1].encode('utf-8'))
    # parsing data into a dictionary
    dfs = dfs.map(lambda raw: ast.literal_eval(raw))
    # filtering data on tweets that are not in English
    dfs = dfs.filter(lambda dictionary: dictionary.get('lang', '') == 'en')
    # filtering data on tweets that contain text
    # this part is a security against empty data
    dfs = dfs.filter(lambda dictionary: dictionary.get('text', '') != '')
    # changing words into tokens
    dfs = dfs.map(lambda dictionary: tokenize(text=dictionary.get('text', ''), common_words=common_words))
    # changing tokens into terms frequency sparse vectors
    dfs = dfs.map(lambda tokens: compute_tf(tokens, reference_table=reference_table))

    # making predictions using the logistic regression
    dfs_predictions = lr.predictOn(dfs)
    # preparing data to count positive and negative predictions
    dfs_predictions = dfs_predictions.map(lambda x: (x, 1))
    # computing positive and negative predictions counts
    dfs_predictions = dfs_predictions.reduceByKey(lambda x, y: x+y)

    # printing the results in the console
    dfs_predictions.pprint()

    # saving data into HBase
    dfs_predictions.foreachRDD(put_data_into_hbase)

    # starting streaming
    ssc.start()
    ssc.awaitTermination()
