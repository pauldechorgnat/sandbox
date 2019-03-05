from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.mllib.linalg import SparseVector
from pyspark.streaming.kafka import KafkaUtils
from nltk.corpus import stopwords
from pyspark.mllib.classification import StreamingLogisticRegressionWithSGD
from sklearn.linear_model import LogisticRegression
# from kafka import SimpleClient, SimpleProducer
from scipy.sparse import csr_matrix
import numpy as np
import json
import re
import datetime
import happybase


def tokenize(text, stop_words, common_words):
    # defining regular expressions
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


def bulk_load(rdd):
    # Your configuration will likely be different. Insert your own quorum and parent node and table name
    conf = {"hbase.zookeeper.qourum": "localhost:2222",
            "zookeeper.znode.parent": "/hbase",
            "hbase.mapred.outputtable": "test",
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}

    key_conv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    value_conv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    load_rdd = rdd.flatMap(parsing_data_for_hbase) #  Convert the CSV line to key value pairs
    load_rdd.saveAsNewAPIHadoopDataset(conf=conf, keyConverter=key_conv, valueConverter=value_conv)


def parsing_data_for_hbase(rdd):
    pass


def create_logistic_regression_skl(weights, intercept):
    n_features = len(weights)
    model = LogisticRegression()
    X = np.random.uniform(size=(2, n_features))
    y = [0, 1]
    model.fit(X, y)
    model.coef_.setfield(weights, dtype=np.float)
    model.intercept_.setfield(intercept, dtype=np.float)

    return model


def predict_skl(features, model):
    predictions = model.predict(csr_matrix(features))
    return predictions


def append_key_to_dictionary(dictionary, key, value):
    dictionary[key] = value
    return dictionary

def insert_into_table(values, table_name, host, port):
    pass



if __name__ == '__main__':

    sc = SparkContext(appName='PythonSparkStreamingKafka')
    sc.setLogLevel("WARN")  # avoid printing logs

    # setting up a model
    lr = StreamingLogisticRegressionWithSGD()
    parameters = json.load(open('model.json', 'r'))
    # lr.setInitialWeights(parameters['weights'])
    lr = create_logistic_regression_skl(parameters['weights'], parameters['intercept'])
    stop_words = load_stopwords()
    common_words = load_common_words()
    reference_table = create_hash_table(common_words=common_words, stop_words=stop_words)

    ssc = StreamingContext(sparkContext=sc, batchDuration=2)
    spark_sql = SQLContext(sparkContext=sc)

    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,
                                                topics=['trump'],
                                                kafkaParams={"metadata.broker.list": 'localhost:9092'})

    dfs = kafkaStream.\
        map(lambda stream: stream[1]).\
        map(lambda raw: json.loads(raw)).\
        filter(lambda dictionary: dictionary.get('lang', '') == 'en').\
        filter(lambda dictionary: dictionary.get('text', '')!='').\
        map(lambda dictionary: append_key_to_dictionary(dictionary,
                                                        key='tokens',
                                                        value=tokenize(text=dictionary.get('text', ''),
                                                                       stop_words=stop_words,
                                                                       common_words=common_words))).\
        map(lambda dictionary: append_key_to_dictionary(dictionary,
                                                        key='features',
                                                        value=compute_tf(dictionary.get('tokens', ''),
                                                                         reference_table=reference_table))).\
        map(lambda dictionary: append_key_to_dictionary(dictionary, key='predictions',
                                                        value=predict_skl(features=dictionary['features'],
                                                                          model=lr)))
    dfs_output = dfs.map(lambda dictionary: str(dictionary['predictions'][0]) + ' - ' + dictionary.get('text'))


    # def emit_to_kafka(predictions):
    #     records = predictions.collect()
    #     for record in records:
    #         producer.send_messages('predictions', str(record).encode('utf-8'))
    #
    #
    # dfs_kafka = dfs_output.map(lambda text: (text[0], 1)).\
    #     reduceByKey(lambda x, y: x+y).\
    #     foreachRDD(emit_to_kafka)
    #
    # kafka_client = SimpleClient('localhost:9092')
    # producer = SimpleProducer(kafka_client)

    dfs_output.pprint(5)

    ssc.start()
    ssc.awaitTermination()
