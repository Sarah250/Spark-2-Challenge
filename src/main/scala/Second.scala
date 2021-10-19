import org.apache.hadoop.fs._
import org.apache.spark.sql.{SQLContext, SparkSession}

//Part 2
object Second {
    def run(sqlContext: SQLContext, spark: SparkSession) : Unit = {
      val pathFile = "src/main/resources/googleplaystore.csv"

      val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("header",true)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("delimiter",",")
        .option("inferSchema",true)
        .load(pathFile)

      val dfAux = df.na.fill(0.0)

      dfAux.createOrReplaceTempView("ratings")

      val df2 = spark.sql("SELECT App, Rating from ratings where Rating >= 4.0 Order by Rating DESC")


      df2.repartition(1)
        .write
        .mode("overwrite")
        .format("com.databricks.spark.csv")
        .options(Map("delimiter"->"§","header"->"true"))
        .save("src/main/result/2_rating")

      val sc = spark.sparkContext

      val fs = FileSystem.get(sc.hadoopConfiguration)

      val file = fs.globStatus(new Path("src/main/result/2_rating/part*"))(0).getPath().getName()

      fs.rename(new Path("src/main/result/2_rating/" + file), new Path("best_apps.csv"))
    }
}
