package part2df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val src = "src/main/resources/data"

  // NOTE: Aggregation & Grouping are Wide transformations
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/movies.json")

  // Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")) // count all the rows, and will include nulls

  // count distinct
  moviesDF.select(countDistinct(col("Major_Genre")))
  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))
  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")
  // sum
  moviesDF.select(sum("US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")
  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")
  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // include null
    .count() // select count(*) from movieDF group by Major_Genre

  val avgRating = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")


  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*") as "N_Movies",
      avg("IMDB_Rating") as "Avg_Rating"
    )
    .orderBy(col("Avg_Rating"))
  aggregationsByGenreDF.show()

  /*
  * Exercises
  * 1. Sum up all the profits of all the movies in the DF
  * 2. Count hw many distinct directors we have
  * 3. Show the mean and standard deviation of us gross revenue for the movie
  * 4. Compute the average IMDB rating & the avg US gross revenue Per director*/

  //1
   moviesDF
    .select(
      (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
        .as("Total_Gross")
    )
    .select(sum("Total_Gross"))
    .show()
  // 2
  moviesDF.select(countDistinct(col("Director")))
    .show()
  // 3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()
  // 4
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating") as "Avg_Rating",
      sum("US_Gross") as "Total_US_Gross"
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}
