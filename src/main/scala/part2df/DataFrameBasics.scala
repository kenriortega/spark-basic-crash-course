package part2df

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFrameBasics extends App {
  val src = "src/main/resources/data"
  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrame Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading DF
  val firstDf = spark.read
    .format("json")
    .option("inferSchema", "true") // in production not be useful
    .load(s"$src/cars.json")

  firstDf.show()
  firstDf.printSchema()

  // get rows
  firstDf.take(10).foreach(println)

  //  spark types
  val longType = LongType

  // schema best practices
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDFSchema = firstDf.schema
  // read df with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load(s"$src/cars.json")

  val myRow = ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")
  // create Df from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17, 8, 302, 140, 3449, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15, 8, 429, 198, 4341, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14, 8, 454, 220, 4354, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14, 8, 440, 215, 4312, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14, 8, 455, 225, 4425, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15, 8, 390, 190, 3850, 8.5, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred
  // note: DFs have schemas, rows do not
  // create df with implicits

  import spark.implicits._

  val manualCarsDFWithImplicits =
    cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /*
  * Exercise:
  * 1. Create a manual DF describing smartphones
  *  - make
  *  - model
  *  - screen dimension
  *  - camera mega pixels
  *
  * 2. Read another file from the data folder
  *   - movies.json
  *   - print its schema
  *   - count the number of rows, call count()
  * */


  // 1
  val smartphones = Seq(
    ("Iphone", "iphone pro Max", 1200, 307),
    ("Iphone", "iphone pro 13", 900, 307),
    ("Iphone", "iphone pro 10", 800, 307)
  )

  //  import spark.implicits._
  val dfSmartphone = smartphones.toDF("Make", "Model", "Screen_Dimension", "Camera_Mega_Pixel")
  dfSmartphone.show()

  // 2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(s"$src/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())
}
