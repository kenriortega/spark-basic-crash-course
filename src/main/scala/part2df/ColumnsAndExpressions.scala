package part2df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")
  // selecting (projecting)
  var carsNamesDF = carsDF.select(firstColumn)

  // various selected methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-convert to column
    $"Horsepower", // fancier interpolated string, returns a columns object
    expr("Origin") // expression
  )
  // other methods
  carsDF.select("Name", "Year")
  // Expressions
  val simplesExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression as "Weight_in_kg",
    expr("Weight_in_lbs / 2.2") as "Weight_in_kg_2"
  )
  // select expr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  // adding a new columns
  val carsWithKgDF3 = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expr
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF
    .filter(
      (col("Origin") === "USA") and (col("Horsepower") > 150)
    )
  val americanPowerfulCarsDF3 = carsDF.filter(
    "Origin = 'USA' and Horsepower > 150"
  )

  //  unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/more_cars.json")
  val allCarsDF = carsDF union moreCarsDF // works if the DFs have the same schema
  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show

  /*Exercise
  * 1- Read the movie DF and select 2 columns of your choice
  * 2- Create another column summing up the total profit of the movies =  US_Gross+Worldwide_Gross+DVD_sales
  * 3- Select all Comedy movies with rating IMDB above 6
  *  use as many versions as possibles
  * */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/movies.json")

  val movieReportDF = moviesDF
    // 1
    .select("Title", "IMDB_Rating")
  // 2
  val totalProfit = moviesDF.col("US_Gross") + moviesDF.col("Worldwide_Gross")

  val movieReportDF2 = moviesDF
    .selectExpr(
      "Title",
      "US_Gross",
      "Worldwide_Gross",
      "US_DVD_Sales",
      "Major_Genre",
      "IMDB_Rating",
      "US_Gross + Worldwide_Gross as Total_Profit" // alternative
    )
  //    .withColumn("Total_Profit", totalProfit) // alternative

  // 3
  val comedyMoviesDF = movieReportDF2.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  comedyMoviesDF.show()

}
