
import sys, datetime
from operator import add

from pyspark.sql import SparkSession


def f(_):
    return _ + 's'


def f2(_):
    return '{}: {}'.format(_, len(_))


def parse_log(log):
    print (log[0], log[3])
    # return (log[0], datetime.strptime(log[3].replace("[", ""), '%d/%b/%Y:%H:%M:%S'))
    return (log[0], log[3])

if __name__ == "__main__":

    if True:
        spark = SparkSession \
            .builder \
            .appName("PythonMongoConnection") \
            .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.random") \
            .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.random") \
            .getOrCreate()

        # people = spark.createDataFrame(
        #     [("Bilbo Baggins", 50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
        #      ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
        raw_rdd = spark.sparkContext.textFile("/Users/Yasir/Downloads/weblogs.txt").map(lambda line: line.split(" ")).map(parse_log)

        # lines = spark.read.text("/Users/Yasir/Downloads/weblogs.txt").rdd.map(parse_log)
        # enTuples = spark.sparkContext.textFile("/Users/Yasir/Downloads/weblogs.txt").map(lambda line: line.split('\n'))
        # weblogdata = enTuples.map(parse_log)
        df = spark.createDataFrame(raw_rdd, ["ip", "date"])

        df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
    else:
        spark = SparkSession\
            .builder\
            .appName("PythonWordCount")\
            .getOrCreate()

        dataArray = ['d1', 'd2', 'd3']
        rdd2 = spark.sparkContext.parallelize(dataArray)
        for a in rdd2.collect():            print a
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