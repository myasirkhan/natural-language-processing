
import sys
from datetime import datetime
from operator import add

from pyspark.sql import SparkSession


def f(_):
    return _ + 's'


def f2(_):
    return '{}: {}'.format(_, len(_))


def parse_log(log):
    try:
        # [u'323.81.303.680', '-', '-', 3'[25/Oct/2011:01:41:00', 4'-0500]', 5'"GET', 6'/download/download6.zip', 'HTTP/1.1"', 8'200', u'0', u'"-"', 11'"Mozilla/5.0', u'(Windows;', u'U;', 14'Windows', 15'NT', 16'5.1;', u'en-US;', u'rv:1.9.0.19)', u'Gecko/2010031422', 18'Firefox/3.0.19"']
        # print ('ip_address: {}, date: {}, method: {}, path: {}, response_code: {}, browser: {}, operating_system: {}, browser_version: {}'.format(log[0], datetime.strptime(log[3][1:], '%d/%b/%Y:%H:%M:%S'), log[5][1:], log[6], log[8], log[11], '{} {}, {}'.format(log[14], log[15], log[16]), log[20]))
        try:
            ip = log[0]
        except:
            ip = None
        try:
            time_str = datetime.strptime(log[3][1:], '%d/%b/%Y:%H:%M:%S')
        except:
            time_str = None
        try:
            method = log[5][1:]
        except:
            method = None
        try:
            path = log[6]
        except:
            path = None
        try:
            response_code = log[8]
        except:
            response_code = None
        try:
            browser = log[11][1:]
        except:
            browser = None
        try:
            operating_system = '{} {}, {}'.format(log[14], log[15], log[16])
        except:
            operating_system = None
        try:
            browser_version = log[20]
        except:
            browser_version = None
        return (ip, time_str, method, path, response_code, browser, operating_system, browser_version)
    except:
        print(log)
        return (log[0], datetime.strptime(log[3][1:], '%d/%b/%Y:%H:%M:%S'), None, None, None, None, None, None)

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
        df = spark.createDataFrame(raw_rdd, ['ip_address', 'date', 'method', 'path', 'response_code', 'browser', 'operating_system', 'browser_version'])

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