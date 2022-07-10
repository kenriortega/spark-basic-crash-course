package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  val src = "src/main/resources/data"
  val spark = SparkSession.builder()
    .appName("ManagingNulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/movies.json")

  // select the first non-null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)
  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
  // removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls
  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show()
  moviesDF.na.fill(Map(
    "Rotten_Tomatoes_Rating" -> 0,
    "IMDB_Rating" -> 0,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // return null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first !=null)second else third
  )
}
