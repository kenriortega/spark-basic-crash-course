package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.util.Date

object DataSets extends App {
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("DataSets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$src/numbers.csv")

  numbersDF.printSchema()
  // convert DF to Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ < 100)

  // dataset of a complex type
  // 1- Define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Option[Long],
                  Displacement: Option[Double],
                  Horsepower: Option[Long],
                  Weight_in_lbs: Option[Long],
                  Acceleration: Option[Double],
                  Year: String,
                  Origin: String
                )

  // 2- read the DF from the file
  def readDf(filename: String) =
    spark.read.option("inferSchema", "true").json(s"$src/$filename")



  // 3 - Define an encoder (import the implicits)

  import spark.implicits._
  //  implicit val carEncoder = Encoders.product[Car] [use import spark.implicits._ better]

  val carsDF = readDf("cars.json")
  // 4- convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100).show()
  // map, flatMap, fold, reduce, for comprehensions ...
  val carsNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carsNamesDS.show

  /*Exercises
  * 1- Count how many cars we have
  * 2- Count how many Powerful cars we have (HP < 140)
  * 3- Avg HP for the entire dataset*/

  //1
  val carsCount = carsDS.count
  println(carsCount)
  //2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)
  //3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

}
