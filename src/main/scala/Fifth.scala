import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Fifth {
  def run( spark: SparkSession, df3 : DataFrame) : Unit = {

    val df4 = df3.withColumn("Genres",explode(col("Genres")))
      .withColumnRenamed("Genres","Genre")
      .groupBy("Genre")
      .agg(
        functions.count("Genre").as("Count"),
        functions.avg("Rating").as("Average_Rating")
      )


    df4.repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("src/main/result/5_metrics")

    val sc = spark.sparkContext

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val file = fs.globStatus(new Path("src/main/result/5_metrics/part*"))(0).getPath().getName()

    fs.rename(new Path("src/main/result/5_metrics/" + file), new Path("googleplaystore_metrics.gz.parquet"))
  }

}
