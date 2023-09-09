from pyspark.sql import SparkSession

if __name__=='__main__':
    spark = (SparkSession.builder
             .appName("reading_images")
             .master("local")
             .getOrCreate())

    df_images = (spark.read.format("image")
                 .load("../data/cctvVideos/train_images/"))

    df_images.printSchema()
    df_images.select("image.origin", "image.height", "image.width", "image.nChannels", "image.mode", "label").show(10, False)