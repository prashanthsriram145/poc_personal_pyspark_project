from pyspark.sql import SparkSession

def create_tables_views(spark):
    spark.sql(""" create database if not exists my_spark_db """)

    spark.sql(""" create table if not exists my_spark_db.managed_flights_table 
    (date int,delay int,distance int,origin string,destination string)""")

    schema = 'date int,delay int,distance int,origin string,destination string'

    df = (spark.read
          .format("csv")
          .schema(schema)
          .load("../data/flights/departuredelays.csv"))
    df.write.mode("overwrite").saveAsTable("my_spark_db.managed_flights_table")

    new_df = spark.table("my_spark_db.managed_flights_table")
    new_df.show(10)

    # spark.sql("use my_spark_db")
    # spark.sql("""CREATE TABLE if not exists us_unmanaged_delay_flights_tbl(date STRING, delay INT,
    #   distance INT, origin STRING, destination STRING)
    #   USING csv OPTIONS (PATH
    #   '../data/flights/departuredelays.csv')""")
    #
    # (df
    #  .write
    #  .option("path", "/tmp/data/us_flights_delay")
    #  .saveAsTable("us_unmanaged_delay_flights_tbl"))

    spark.sql(""" create or replace global temp view global_temp_view as 
    select * from my_spark_db.managed_flights_table where origin='SFO'""")

    spark.sql(""" create or replace temp view temp_view_us_flights as 
    select * from my_spark_db.managed_flights_table where origin='SFO'""")

    print(spark.catalog.listDatabases())
    print(spark.catalog.listTables('my_spark_db'))
    print(spark.catalog.listTables('default'))

    spark.sql(" select * from global_temp.global_temp_view").show(10)
    spark.sql(" select * from temp_view_us_flights").show(10)



if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('managed_unmanaged_tables_views')
             .config("spark.sql.warehouse.dir", "C:\code\pyspark-spk-learning\spark-warehouse")
             .master('local')
             .enableHiveSupport()
             .getOrCreate())

    create_tables_views(spark)