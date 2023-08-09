from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_timestamp, year

def sf_fire_calls_reader(spark):
    schema = "CallNumber int,UnitID string,IncidentNumber int,CallType string,CallDate string,WatchDate string," \
             "CallFinalDisposition string,AvailableDtTm string,Address string,City string,Zipcode int,Battalion string," \
             "StationArea string,Box string,OriginalPriority string,Priority string,FinalPriority int,ALSUnit boolean," \
             "CallTypeGroup string,NumAlarms int,UnitType string,UnitSequenceInCallDispatch int," \
             "FirePreventionDistrict string,SupervisorDistrict string,Neighborhood string,Location string,RowID string,Delay double"

    spark.sql("create database if not exists spark_db")
    df = spark.read.format("csv").schema(schema).option("header", "true").load("../data/sf-fire-calls.csv")

    print(df.schema.simpleString())

    few_fire_df = (df.select("IncidentNumber", "CallType", "AvailableDtTm")
                   .where(col("CallType").like("Medical Incident")))
    few_fire_df.show(5)


    distinct_calltypes_df = (df.select("CallType")
     .where(expr("CallType is not null"))
    .distinct())
    distinct_calltypes_df.show(5, False)
    print('Distinct Call types: ', distinct_calltypes_df.count())


    column_renamed_df = df.withColumnRenamed("Delay", "DelayInResponse")
    column_renamed_df.show(5)


    fire_ts_df = (df.withColumn("IncidentDate", to_timestamp("CallDate", "MM/dd/yyyy"))
                  .drop("CallDate")
                  .withColumn("WatchedDate", to_timestamp("WatchDate", "MM/dd/yyyy"))
                  .drop("WatchDate")
                  .withColumn("AvailableTimestamp", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
                  .drop("AvailableDtTm")
                  .withColumn("IncidentYear", year("IncidentDate")))
    fire_ts_df.show(5, False)


    (fire_ts_df.select(year("IncidentDate"))
     .distinct()
     .orderBy(year('IncidentDate'))
     .show(10))


    (fire_ts_df.select("CallType")
     .where(col("CallType").isNotNull())
     .groupby("CallType")
     .count()
     .orderBy("count", ascending=False)
     .show(10))

    (fire_ts_df.select("IncidentYear")
     .groupby("IncidentYear")
     .count()
     .orderBy("count", ascending=False)
     .show(10))


    df.write.format("parquet").mode("overwrite").save("../data/sf-fire-calls-parquet")

    df.write.format("parquet").mode("overwrite").saveAsTable('spark_db.sf_fire_calls_table')

    new_df = spark.table('spark_db.sf_fire_calls_table')
    new_df.show(10)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("sf_fire_calls_reader")\
        .master("local")\
        .config("spark.sql.warehouse.dir", "C:\code\pyspark-spk-learning\spark-warehouse") \
        .enableHiveSupport()\
        .getOrCreate()

    sf_fire_calls_reader(spark)