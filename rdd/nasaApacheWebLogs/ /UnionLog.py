from pyspark import SparkContext, SparkConf


def isNotHeader(line: str):

    return not(line.startswith("host") and "bytes" in line)




if __name__ == "__main__":

    """

    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's
    apache server for july 1st 1995

    "nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a spark program to generate a new RDD which contains the log lines 
    from both July 1st and August 1st, take a 0.1 sample of those log lines 
    and save it to "out/ample_nasa_logs.tsv" file

    """

    '''
    Keep in mind, that the original log files contains the following header lines.
    host  logname  time  method  url  response  bytes

    Make sure the head lines are in the resulting RDD
    '''

    conf = SparkConf()\
            .setAppName("UnionLog")\
            .setMaster("local[*]")

    sc = SparkContext(conf = conf)

    julio = sc.textFile("in/nasa_19950701.tsv")
    agosto = sc.textFile("in/nasa_19950801.tsv")

    ju = julio.filter(isNotHeader)
    ag = agosto.filter(isNotHeader)

    julioAgsto = ju.union(ag)
    sampleJA = julioAgsto.sample(withReplacement = True, fraction = 0.1)

    sampleJA.saveAsTextFile("out/sample_nasa_logs.tsv")
