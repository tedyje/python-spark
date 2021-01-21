from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    conf = SparkConf()\
            .setAppName("Word Count")\
            .setMaster("local[3]")


    sc = SparkContext(conf = conf)

    lines = sc.textFile("./in/word_count.text")
    words = lines.flatMap(lambda arg: arg.rstrip().split(" "))

    wordsCount = words.countByValue()

    for word, value in wordsCount.items():
        print("{}: {}".format(word, value))
