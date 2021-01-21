from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    """
    Create a Spark program to read the article from in/word_count.text,
    output the number of occurrence of each word in descending order.

    Sample output:

    apple : 200
    shoes : 193
    bag : 176
    """

    conf = SparkConf() \
            .setAppName("SortedWordsCount") \
            .setMaster("local[*]")

    sc = SparkContext(conf = conf)


    lines = sc.textFile("in/word_count.text") 

    wordRdd = lines.flatMap(lambda line: line.split(" "))
    wordRddMap = wordRdd.map(lambda word: (word.lower(), 1))

    wordCountRdd = wordRddMap \
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda wordCount: wordCount[1], ascending = False)

    for word, count in wordCountRdd.collect():
        print("{} : {}".format(word, count))

    
