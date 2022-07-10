package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, expr, size, split, struct, to_date}

object ComplexTypes extends App {
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/movies.json")


  // Dates
  val moviesWithReleaseDates = moviesDF.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"),
  )

  moviesWithReleaseDates.withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)

  /*
  * Exercise
  * 1. How do we deal with multiple date formats
  * 2. Read the stocks DF and parse the dates
  * */

  // 1- parse the DF multiples times , the union the small DFs

  // 2
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(s"$src/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  // Structures
  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross") as "US_Profit")

  // 2- with expressions strings
  moviesDF.selectExpr("Title", "(US_Gross,Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")


  // Arrays
  val moviesWithWords= moviesDF.select(col("Title"),split(col("Title")," |,") as "Title_Words") // array of strings
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"),"Love")
  )


}
