from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    conf = SparkConf() \
            .setAppName("Create") \
            .setMaster("local")


    sc = SparkContext(conf = conf)

    inputStrings = ["Lily 23", "Jack 29",  "May 29", "James 8"]
    regualrRDDs = sc.parallelize(inputStrings)


    pairRDD = regualrRDDs.map(lambda s: (s.split(" ")[0], s.split(" ")[1]))


    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd")

    



