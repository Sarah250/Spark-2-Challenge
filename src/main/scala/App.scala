import org.apache.spark.sql.SparkSession


object App {

  def main(args : Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\handoop")

    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    //Part 1
    val df1 = First.run(sqlContext)
    //Part 2
    Second.run(sqlContext,spark)
    //Part 3
    val df3 =  Third.run(sqlContext,spark)
    //Part 4
    Fourth.run(spark,df1,df3)
    //Part 5
    Fifth.run(spark,df3)

    // Parket reader
    //ParketReader.read(spark)

  }

}

