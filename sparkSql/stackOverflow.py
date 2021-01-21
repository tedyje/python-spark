from pyspark.sql import SparkSession


AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"


if __name__ == "__main__":

    session = SparkSession\
            .builder\
            .appName("StackOverFlowSurvey")\
            .master("local[*]")\
            .getOrCreate()

    responses = session\
            .read.option("header", "true")\
            .option("inferSchema", value = True)\
            .csv("in/2016-stack-overflow-survey-responses.csv")

    print("=== Print out Schema  ===")
    responses.printSchema()


    responseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)

    print("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show()


    print("=== Print records where the response is from Afghanistan ===")
    responseWithSelectedColumns\
            .filter(responseWithSelectedColumns["country"] == "Afghanistan").show()


    print("=== Print the count of occupation ===")
    groupedData = responseWithSelectedColumns\
            .groupBy("occupation")
    
    groupedData\
            .agg({"occupation": "count"})\
            .show()


    print("=== Print records with average mid age less than 20 ===")
    responseWithSelectedColumns\
            .filter(responseWithSelectedColumns[AGE_MIDPOINT] > 20).show()

    print("=== Print the result by salary middle point in descending order ===")
    
    responseWithSelectedColumns\
            .orderBy(SALARY_MIDPOINT, ascending = False)\
            .show()

    print("=== Group by country and aggregate by average salary middle point ===")

    responseWithSelectedColumns\
            .groupBy("country")\
            .agg({SALARY_MIDPOINT: "avg"})\
            .show()

    responseWithSalaryBucket = responses\
            .withColumn(SALARY_MIDPOINT_BUCKET, (responses[SALARY_MIDPOINT]/20000).cast("integer")*20000)

    print("=== With Salary bucket column ===")
    responseWithSalaryBucket\
            .select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET)\
            .show()


    print("=== Group by salary bucker column ===")
    responseWithSalaryBucket\
            .groupBy(SALARY_MIDPOINT_BUCKET)\
            .count()\
            .orderBy(SALARY_MIDPOINT_BUCKET)\
            .show()

    session.stop()




