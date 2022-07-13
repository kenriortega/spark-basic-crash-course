package part5rdd

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDD extends App {
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("RDD")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1- parallelize an existing collections
  val numbers = 1 to 1_000_000
  val numberRDD = sc.parallelize(numbers, numSlices = 8)

  // 2- reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStock(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1) // remove header
      .map(_.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(
    readStock(s"$src/stocks.csv"),
    numSlices = 8,
  )

  // 2b - reading from files
  val stocksRdd2 = sc.textFile(s"$src/stocks.csv")
    .map(_.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3- read from a DF
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(s"$src/stocks.csv")

  // lose a type information
  val stocksRDD4 = stocksDF.rdd

  import spark.implicits._

  val stocksDS = stocksDF.as[StockValue]
  val stockRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numberRDD.toDF("numbers") // you lose the type information
  // RDD -> DS
  val numbersDS = spark.createDataset(numberRDD) // keep the type information

  // transformations
  val msftRdd = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRdd.count() // eager action

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy
  // min & max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRdd.min() // action
  // reduce
  numberRDD.reduce(_ + _)
  val groupedStockRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive

  // Partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet(s"$src/stock30")
  /*Repartition is expensive. Involves shuffling
  * Best practice: partition early, then process that
  * Size og a partition 10 - 100MB Best practice
  * */

  // coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does not involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet(s"$src/stock15")


  /*Exercise
  * 1- Read the movie.json as an RDD
  * 2- show distinct genre as an RDD
  * 3- Select all the movie in the Drama genre with IMDB rating > 6
  * 4- Show the avg rating of movies by genre.
  * */
  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/movies.json")
  val moviesRDD = moviesDF
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    ).where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct
  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  moviesRDD.toDF.show
  genresRDD.toDF.show
  goodDramasRDD.toDF.show

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre = genre, rating = movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show
}
