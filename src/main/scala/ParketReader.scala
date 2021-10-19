import org.apache.spark.sql.SparkSession

object ParketReader {

  def read(spark: SparkSession) : Unit ={
    val pathFile = "src/main/resources/googleplaystore_cleaned.gz.parquet"
    val df = spark.read.parquet(pathFile)

    df.show(100)
  }
}
