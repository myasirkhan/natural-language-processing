
import sys
from operator import add

from pyspark.sql import SparkSession


def f(_):
    return _ + 's'

def f2(_):
    return '{}: {}'.format(_, len(_))

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    dataArray = ['d1', 'd2', 'd3']
    rdd2 = spark.sparkContext.parallelize(dataArray)
    for a in rdd2.collect():
        print a
    string = ["hasdasdasd___"]
    include_s = spark.sparkContext.parallelize(string).map(f).collect()
    print "With s included: {}".format(include_s)

    string_lst = ["hasdasdasd___", "asdasd", "2fefwef"]
    str_counts = spark.sparkContext.parallelize(string_lst).map(f2).collect()
    print "Word letters count: "
    for s in str_counts:
        print s

    if len(sys.argv) != 2:
        print("Usage: wordcount <file>")
        exit(-1)

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    counts = lines.flatMap(lambda x: x.split(' '))\
                  .map(lambda x: (x, 1))\
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("{}: {}".format(word, count))

    spark.stop()