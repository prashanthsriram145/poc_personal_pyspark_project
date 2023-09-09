from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def read_flights_data(spark):
    schema = "date int,delay int,distance int,origin string,destination string"

    df = (spark
          .read.format("csv")
          .schema(schema)
          .option("header", "true")
          .load("../data/flights/departuredelays.csv"))

    df.createOrReplaceTempView("flights_data_view")

    spark.sql("select * from flights_data_view where distance > 1000 order by distance desc").show(10)

    (df.select("*")
     .where(col("distance") > 1000)
     .orderBy(col("distance").desc()).show(10))

    spark.sql("select * from flights_data_view where origin='SFO' and destination='ORD' and delay > 120 order by delay desc").show(10)

    (df.select("*")
     .where(expr("origin='SFO' and destination='ORD' and delay > 120"))
     .orderBy(col("delay").desc())
     .show(10))

    spark.sql(""" select *, 
     case 
     when delay > 360 then 'very delayed'
     when delay >= 120 and delay <= 360 then 'delayed'
     when delay >= 60 and delay < 120 then 'little delay'
     when delay > 0 and delay < 60 then 'tolerable delay'
     when delay = 0 then 'No delay'
     else 'early'
     end as flight_delay
     from flights_data_view
     order by origin, delay desc 
    """).show(10)

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("flights_data_reader")
             .master("local")
             .getOrCreate())

    read_flights_data(spark)