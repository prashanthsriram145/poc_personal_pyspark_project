from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

import pandas as pd
from pyspark.sql.functions import pandas_udf, col

def cubed(s):
    return s * s * s

def cubed_pd(a: pd.Series) -> pd.Series:
    return a * a * a

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("spark_udf_samples")
             .master("local")
             .getOrCreate())

    spark.udf.register("cubed", cubed, LongType())

    spark.range(1,10).createOrReplaceTempView("cubed_df")

    spark.sql("select id, cubed(id) from cubed_df").show(10)

    cubed_udf = pandas_udf(cubed_pd, returnType=LongType())

    # df = spark.range(1, 4)
    #
    # # Execute function as a Spark vectorized UDF
    # df.select("id", cubed_udf(col("id"))).show()