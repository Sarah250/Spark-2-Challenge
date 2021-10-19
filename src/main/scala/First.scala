import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType}

//Part 1
object First {

  def run(sqlContext: SQLContext) : DataFrame = {
    val pathFile = "src/main/resources/googleplaystore_user_reviews.csv"

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header",true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("delimiter",",")
      .option("inferSchema",true)
      .load(pathFile)

    val df1 = df.na.replace("Sentiment_Polarity",Map("nan"->"0.0"))
                      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))
                      .groupBy("App").avg("Sentiment_Polarity")
                      .withColumnRenamed("avg(Sentiment_Polarity)","Average_Sentiment_Polarity")

    return df1
  }

}
