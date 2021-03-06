package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lit, not, regexp_extract, regexp_replace}
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("Common Spark types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/movies.json")

  // Adding a common value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(dramaFilter)
  // + multiples ways of filtering
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movies"))
  moviesWithGoodnessFlagsDF.where("good_movies") //eq -> where(col("good_movies")) === true

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movies")))
  // Numbers
  // math operators
  val moviesAvgRatingsDF = moviesDF.select(
    col("Title"),
    (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
  )
  // correlations = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /*corr is an Action*/)

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/cars.json")

  // capitalization: initCap, lower, upper
  carsDF.select(initcap(col("Name")))
  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))
  // regex
  val regexString = "volkswagen|vw"
  val wdDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0) as "regex_extract"
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  // replacing
  wdDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People`s Car") as "regex_replace"
  )

  /*
  * Exercise
  *  filter the cars DF by a list of car names obtained by an API call
  *  Versions:
  *   - contains
  *   - regex
  *
  * */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase).mkString("|")
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0) as "regex_extract"
  )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  // version 2 - contains
  var carNameFilters = getCarNames.map(_.toLowerCase).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show
}
