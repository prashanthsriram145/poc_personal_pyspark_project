from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_list, get_json_object

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("higher_order_functions")
             .master("local")
             .getOrCreate())

    data = [[1, [1,2,3,4]], [2, [2,3,4,5]]]
    schema = "id int, values array<int>"

    df = spark.createDataFrame(data, schema)
    df.show(10)
    df.createOrReplaceTempView("df_table")

    spark.sql("""
    select id, collect_list(value+1) from 
    (select id, explode(values) as value from df_table) 
    group by id
    """).show(10)

    data = [("1", '''{"f1": "value1", "f2": "value2"}'''), ("2", '''{"f1": "value12"}''')]
    df = spark.createDataFrame(data, ("key", "jstring"))
    df.select(df.key, get_json_object(df.jstring, '$.f1').alias("c0"), get_json_object(df.jstring, '$.f2').alias("c1") ).show(10)