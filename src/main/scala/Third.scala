import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_set, date_format, regexp_replace, round, row_number, split, to_date}
import org.apache.spark.sql.types.{DoubleType, LongType}


object Third {

  def run(sqlContext : SQLContext, spark: SparkSession)  : DataFrame = {
    val pathFile = "src/main/resources/googleplaystore.csv"

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header",true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("delimiter",",")
      .option("inferSchema",true)
      .load(pathFile)

    // Selecting maximum reviews
    val windowDept = Window.partitionBy("App").orderBy(col("Reviews").desc)


    val dfAux = df
      .withColumn("Last Updated", date_format(to_date(col("Last Updated"),"MMMM dd,yyyy"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("Rating",functions.when(functions.isnan(functions.col("Rating")),null).otherwise(functions.col("Rating")))
      .withColumn("Price",
          regexp_replace(df("Price"),"[^. 0-9]","")
        )
      .withColumn("Price", round(col("Price").cast(DoubleType)*0.9,2))
      .withColumn("Genres",split(col("Genres"),";"))
      .withColumn("Size",
          regexp_replace(df("Size"),"[^. 0-9]","")
        )
      .withColumn("Size",col("Size").cast(DoubleType))
      .withColumn("Reviews",col("Reviews").cast(LongType))
      .withColumn("row",row_number.over(windowDept))
      .where(col("row") === 1).drop("row")
      .withColumnRenamed("Content Rating","Content_Rating")
      .withColumnRenamed("Current Ver","Current_Version")
      .withColumnRenamed("Android Ver","Minimum_Android_Version")
      .withColumnRenamed("Last Updated","Last_Updated")

    val dfGroup = df
      .groupBy("App")
      .agg(collect_set("Category").as("Categories"))
      .dropDuplicates("App")

    dfAux.createOrReplaceTempView("all")
    dfGroup.createOrReplaceTempView("all2")

    val df3 =  spark.sql("Select all2.App, all2.Categories, all.Rating, all.Reviews, all.Size, all.Installs, all.Type, all.Price, all.Content_Rating, " +
                                  "all.Genres, all.Last_Updated, all.Current_Version, all.Minimum_Android_Version " +
                                  "FROM all2 " +
                                  "INNER JOIN all ON all2.App = all.App")

    return df3
  }
}
