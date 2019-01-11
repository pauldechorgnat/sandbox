from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils
import json


if __name__ == '__main__':
    sc = SparkContext(appName='PythonSparkStreamingKafka')
    sc.setLogLevel("WARN")  # avoid printing logs
    ssc = StreamingContext(sparkContext=sc, batchDuration=2)
    spark_sql = SQLContext(sparkContext=sc)

    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,
                                                topics=['trump'],
                                                kafkaParams={"metadata.broker.list": 'localhost:9092'})

    dfs = kafkaStream.\
        map(lambda dstream: dstream[1])
    dfs.pprint(2)

    ssc.start()
    ssc.awaitTermination()
