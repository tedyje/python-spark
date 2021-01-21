import sys
sys.path.insert(0, ".")
from commons.Utils import Utils

from pyspark import SparkContext, SparkConf

def pairRdd(line):

    airportPairRdd = Utils.COMMA_DELIMITER.split(line)

    return airportPairRdd[1], airportPairRdd[3]

if __name__ == "__main__":

    """
    Create a spark program to read the airport data from in/airports.text;
    generate a pair RDD with airport name being the key and country name being the value.

    Each row of the input contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located,
    IATA/FAA code, ICAO code, Latitude, Longitude, Altitude, Timezone, dST, Timezone in 
    Oslon format

    Sample output:

        ("Kamloops", "Canada")
        ("Wewak Intl", "Papua New Guinea")
    """

    conf = SparkConf() \
            .setAppName("AirportsNotInUSA") \
            .setMaster("local[2]")

    sc = SparkContext(conf = conf)


    lines = sc.textFile("in/airports.text")

    airportsNotInUSRdd = lines \
            .map(pairRdd) \
            .filter(lambda keyValue: keyValue[1] != "\"United States\"")

    airportsNotInUSRdd.saveAsTextFile("out/airports_not_in_us_pair_rdd.text")

