from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("dataframe_common_operations")
             .config("spark.sql.warehouse.dir", "C:\code\pyspark-spk-learning\spark-warehouse")
             .master("local")
             .enableHiveSupport()
             .getOrCreate())

    flight_delays_df = (spark.read
                        .format("csv")
                        .option("inferSchema", "true")
                        .option("header", "true")
                        .load("../data/flights/departuredelays.csv"))
    flight_delays_df.createOrReplaceTempView("flight_delays")
    foo = flight_delays_df.filter(expr("""origin == 'SEA' and destination == 'SFO'  
    and delay > 0"""))
    foo.show(10)

    airport_codes_df = (spark.read.format("csv")
                        .options(header="true", sep="\t", inferSchema="true")
                        .load("../data/flights/airport-codes-na.txt"))
    airport_codes_df.createOrReplaceTempView("airport_codes")

    spark.sql("select * from flight_delays").show(10)
    spark.sql("select * from airport_codes").show(10)

    bar = flight_delays_df.union(foo)
    print(flight_delays_df.count())
    print(bar.count())
    print(foo.count())

    (flight_delays_df.join(airport_codes_df, airport_codes_df.IATA == flight_delays_df.origin)
     .select("City", "State", "Country", "date", "delay", "distance").show(10))

    spark.sql(""" select a.City, a.State, a.Country, f.date, f.delay, f.distance 
    from flight_delays f join airport_codes a on f.origin=a.IATA limit 10""").show()

    spark.sql("drop table if exists departDelaysWindow")
    spark.sql(""" create or replace temp view departureDelayWindow as 
    select origin, destination, sum(delay) as totaldelays from flight_delays 
    where origin in ('SEA','SFO', 'JFK') and 
    destination in ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') group by origin, destination """)

    spark.sql("select * from departureDelayWindow").show(10)

    spark.sql(""" select origin, destination, totaldelays, rank 
    from (select origin, destination, totaldelays, dense_rank() over 
    (partition by origin order by totaldelays desc) as rank 
    from departureDelayWindow ) t where rank <= 3 """).show()

    spark.sql(""" SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
        FROM flight_delays 
        WHERE origin = 'SEA' """).show()

    spark.sql(""" SELECT * FROM (
    SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
    FROM flight_delays WHERE origin = 'SEA' 
    ) 
    PIVOT (
    CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
    FOR month IN (1 JAN, 2 FEB, 3 MAR, 4 APR, 5 MAY, 6 JUN, 7 JUL, 8 AUG, 9 SEP, 10 OCT, 11 NOV, 12 DEC)
    )
    ORDER BY destination """).show()
