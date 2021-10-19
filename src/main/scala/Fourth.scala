import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object Fourth {

  def run( spark: SparkSession, df1 : DataFrame, df3 : DataFrame) : DataFrame = {
    df1.createOrReplaceTempView("df1")
    df3.createOrReplaceTempView("df3")

    val df4 =  spark.sql("Select df3.*, df1.Average_Sentiment_Polarity " +
      "FROM df3 " +
      "INNER JOIN df1 ON df1.App = df3.App")


    df4.repartition(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("src/main/result/4_cleaned")

    val sc = spark.sparkContext

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val file = fs.globStatus(new Path("src/main/result/4_cleaned/part*"))(0).getPath().getName()

    fs.rename(new Path("src/main/result/4_cleaned/" + file), new Path("googleplaystore_cleaned.gz.parquet"))

    return df4
  }

}
