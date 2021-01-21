import sys
sys.path.insert(0, ".")
from pairRdd.aggregation.reducebyKey.housePrice.avgCount import AvgCount

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    conf = SparkConf()\
            .setAppName("AverageHousePrice")\
            .setMaster("local[*]")

    sc = SparkContext(conf = conf)

    lines = sc.textFile("in/RealEstate.csv")
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)

    housePricePairRdd = cleanedLines.map(lambda line:
                                    ((int(float(line.split(",")[3]))),AvgCount(1, float(line.split(",")[2]))))

    housepriceTotal = housePricePairRdd.reduceByKey(lambda x, y:  x+y) 



