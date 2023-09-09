from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, concat

def blogs_reader(spark):
    schema = "Id long, Hits long, Last string, First string, Campaigns array<string>"
    df = spark.read.format("json").schema(schema).load("../data/blogs.json")
    df.printSchema()
    new_df = df.withColumn("Big_Hitters", expr("Hits > 10000"))
    new_df.show(10, False)
    new_df = df.withColumn("Real_Big_Hitters", col("Hits") > 15000)
    new_df.show(10, False)
    new_df = new_df.withColumn("AuthorsId", concat("First", "Last", "Id"))
    new_df.show(10, False)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("blogs_reader").master("local").getOrCreate()
    blogs_reader(spark)