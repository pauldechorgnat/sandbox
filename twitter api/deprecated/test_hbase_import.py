from pyspark import SparkContext
import pprint


data = [
    (1, 'hello world'),
    (2, 'bonjour monde'),
    (3, 'bon giorno')
]

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


def parsing_data_for_hbase(line):
    # returns data in the form
    # (key, (key, column_family, column_descriptor, value))
    key, arguments = line
    to_return = []
    for index, argument in enumerate(arguments):
        to_return.append((key, (key, 'sentence', 'word' + str(index), argument)))
    return to_return


if __name__ == '__main__':

    sc = SparkContext.getOrCreate()

    rdd = sc.parallelize(data)

    pprint.pprint(rdd.take(3))

    # rdd = rdd.flatMap(parsing_data_for_hbase)
    # pprint.pprint(rdd.take(4))


    hbase_table = 'test'
    import happybase
    h_base_connection = happybase.Connection(host='localhost', port=16010)
    table = h_base_connection.table(hbase_table)

    def save_to_h_base(rdd):
        for line in rdd.collect():
            table.put(

                    ('test' + str(line[0])), {
                        b'mot:premier_mot': (line[1].split(' ')[0]),
                        b'mot:second_mot': (line[1].split(' ')[1])
                    })
    save_to_h_base(rdd=rdd)
