import sys
sys.path.insert(0, ".")

from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def pairRdd (line):

    airportRdd = Utils.COMMA_DELIMITER.split(line)

    return airportRdd[1], airportRdd[3] 

if __name__ == "__main__":

    """
    Create a spark program to read the airport data from in/airports.text, generate a pair 
    RDD with airport name being the key and country name being the value. Then convert the 
    country name to uppercase and output the pair RDD to out/airports_uppercase.text

    Each row of the input file contains the following columns:

    Airport ID, Name of airport, Main city served by airport, Country where airport is located,
    IATA/FAA code, ICAO code, Latitude, Longitude, Altitude, Timezone, DST, Timezone, DST, 
    Timezone in Olson format

    Sample output:

    ("Kamloops", "CANADA")
    ("Wewak Intl", "PAPAUA NEW GUINEA")
    """

    conf = SparkConf() \
            .setAppName("ValueUppercase") \
            .setMaster("local[1]") 

    sc = SparkContext(conf = conf)

    lines = sc.textFile("in/airports.text")

    airportsPairRdd = lines.map(pairRdd)

    airportsCapitalPairsRdd = airportsPairRdd \
            .mapValues(lambda value: value.upper())

    airportsCapitalPairsRdd.saveAsTextFile("out/airports_uppercase.text")
