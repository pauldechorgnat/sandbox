from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import re


if __name__ == '__main__':
    sc = SparkContext(appName='PythonSparkStreamingKafka')
    sc.setLogLevel(logLevel='WARN')

    ssc = StreamingContext(sparkContext=sc, batchDuration=1)
    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc, topics=['trump'],
                                                kafkaParams={"metadata.broker.list": 'localhost:9092'})

    regex = re.compile('\\w+')

    lines = kafkaStream.map(lambda line: json.loads(line[1])).\
        filter(lambda d: d.get('lang', '') == 'en').\
        flatMap(lambda d: regex.findall(d['text'].lower())).\
        map(lambda word: (word, 1)).\
        reduceByKey(lambda x, y: x+y)
    lines.pprint()
    lines.saveAsTextFiles('hdfs:///home/hduser/test_1')
    ssc.start()
    ssc.awaitTermination()
