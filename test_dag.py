from pyspark import SparkContext
import time

if __name__ == '__main__':
    sc = SparkContext()

    data = [i for i in range(1000000)]

    # joining before filtering
    t0 = time.time()
    rdd1 = sc.parallelize(data)
    rdd2 = sc.parallelize(data)

    t1 = time.time()

    rdd1 = rdd1.map(lambda x: (x, 1))
    rdd2 = rdd2.map(lambda x: (x, 1))

    t2 = time.time()

    rdd3 = rdd1.join(rdd2)

    t3 = time.time()

    rdd3 = rdd3.filter(lambda x: x[0] < 10)

    t4 = time.time()

    results1 = rdd3.collect()

    t5 = time.time()

    print('time needed to create rdd: ', t1-t0)
    print('time needed to transform data: ', t2-t1)
    print('time needed to join data: ', t3 - t2)
    print('time needed to filter data: ', t4 - t3)
    print('time needed to collect results: ', t5 - t4)

    sc.stop()

    # filtering before joining

    sc = SparkContext()

    t0 = time.time()
    rdd6 = sc.parallelize(data)
    rdd7 = sc.parallelize(data)

    t1 = time.time()

    rdd6 = rdd6.map(lambda x: (x, 1))
    rdd7 = rdd7.map(lambda x: (x, 1))

    t2 = time.time()

    rdd6 = rdd6.filter(lambda x: x[0] < 10)

    t3 = time.time()

    rdd8 = rdd6.join(rdd7)

    t4 = time.time()

    results2 = rdd8.collect()

    t5 = time.time()
    print()
    print('time needed to create rdd: ', t1 - t0)
    print('time needed to transform data: ', t2 - t1)
    print('time needed to filter data: ', t3 - t2)
    print('time needed to join data: ', t4 - t3)
    print('time needed to collect results: ', t5 - t4)

    print(results1)
    print(results2)
    sc.stop()