import sys
sys.path.insert(0, '.')


from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):

    splits = Utils.COMMA_DELIMITER.split(line)
    
    return "{}, {}".format(splits[1], splits[2])


if __name__ == "__main__":

    conf = SparkConf()\
            .setAppName("Airpors")\
            .setMaster("local[3]")

    sc = SparkContext(conf = conf)

    airports = sc.textFile("in/airports.text")
    airportsInUSA = airports.filter(lambda arg: Utils.COMMA_DELIMITER.split(arg)[3] == "\"United States\"" )


    airportsNameAndCityNames = airportsInUSA.map(splitComma)
    #airportsNameAndCityNames.saveAsTextFile("./out/airports_in_usa")

    for airports in airportsInUSA.take(6):
        print(airports)

