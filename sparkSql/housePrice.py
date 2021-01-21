from pyspark.sql import SparkSession, functions as fs

PRICE_SQ_FT = "Price SQ Ft"


if __name__ == "__main__":

    '''
    Create a Spark program to read the house data from in/RealEstate.csv,
    group by location, aggregate the average price per SQ Ft and sort by
    average price per SQ Ft.

    The houses dataset contains a collection of recent real state listings
    in San Luis Obispo county ana around it

    The dataset contains the following fields:
    1. MLS: Multiple listing service number for th ehouse (unique ID)
    2. Location: city/town where the house is located. Most locations are in
       San Luis Obispo country and nothern Santa Barbara county (Santa MariaOrcutt,
       Lompoc, Guadelupe, Los Alamos), but there some out of area locations as well.
    3. Price: the most recent listing price of the house (in dollars)
    4. Bedrooms: number of bedrooms
    5. Bathrooms: number of bathrooms
    6. Size: size of the house in square feet
    7. Price/SQ.ft: price of the house per square foot
    8. Status: type of sale. The types are represented in the dataset: Short Sale,
       Forclosure and Regualr.

       Each field is comma separated.
    '''

    session = SparkSession\
            .builder\
            .appName("HousePrice")\
            .master("local[*]")\
            .getOrCreate()

    realEstate = session\
            .read\
            .option('header', 'true')\
            .option("inferSchema", value = True)\
            .csv("in/RealEstate.csv")

    realEstate.groupBy("Location")\
            .agg({PRICE_SQ_FT : "avg"})\
            .orderBy("avg(Price SQ Ft)")\
            .show()




