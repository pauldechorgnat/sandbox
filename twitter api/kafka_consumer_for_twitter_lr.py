from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql import SQLContext
import re


if __name__ == '__main__':
    sc = SparkContext(appName='PythonSparkStreamingKafka')
    sc.setLogLevel(logLevel='WARN')
    sql = SQLContext(sc)

    ssc = StreamingContext(sparkContext=sc, batchDuration=1)
    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc, topics=['trump'],
                                                kafkaParams={"metadata.broker.list": 'localhost:9092'})

    regex = re.compile('\\w+')

    lines = kafkaStream.f
    lines.pprint()
    lines.saveAsTextFiles('hdfs:///home/hduser/test_1')
    ssc.start()
    ssc.awaitTermination()
