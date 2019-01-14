from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from nltk.corpus import stopwords
from pyspark.mllib.classification import StreamingLogisticRegressionWithSGD
import json
import re


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


if __name__ == '__main__':

    sc = SparkContext(appName='PythonSparkStreamingKafka')
    sc.setLogLevel("WARN")  # avoid printing logs

    # setting up a model
    lr = StreamingLogisticRegressionWithSGD()
    parameters = json.load(open('model.json', 'r'))
    lr.setInitialWeights(parameters['weights'])

    stop_words = load_stopwords()
    common_words = load_common_words()

    ssc = StreamingContext(sparkContext=sc, batchDuration=2)
    spark_sql = SQLContext(sparkContext=sc)

    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,
                                                topics=['trump'],
                                                kafkaParams={"metadata.broker.list": 'localhost:9092'})

    dfs = kafkaStream.\
        map(lambda stream: stream[1]).\
        map(lambda raw: json.loads(raw)).\
        filter(lambda dictionary: dictionary.get('lang', '') == 'en').\
        map(lambda dictionary: dictionary.get('text', '')).\
        map(lambda text: tokenize(text=text, stop_words=stop_words, common_words=common_words))


    ssc.start()
    ssc.awaitTermination()
