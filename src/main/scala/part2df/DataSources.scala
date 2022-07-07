package part2df

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("Data sources & formats")
    .config("spark.master", "local")
    .getOrCreate()

  // schema best practices
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  * Reading a DF:
  * - format
  * - schema(optional) or inferSchema
  * - zero or more options
  * - path
  * */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed,permissive(default)
    .option("path", s"$src/cars.json")
    //    .option("path", s"$src/cars_fail.json") //Parse Mode: FAILFAST. To process malformed records
    .load() // s3, local pc

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
        "mode" -> "failFast",
        "path" -> s"$src/cars.json",
        "inferSchema" -> "true"
      )
    )
    .load()

  /*Writing DF
  * - format
  * - save mode = overwrite, append,ignore,errorIfExists
  * - path
  * - zero or more options
  * */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save(s"$src/cars_duplicates.json")

  // JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema ; if spark fail parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2,gzip,lz4,snappy,deflate
    .json(s"$src/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY") // couple with schema ; if spark fail parsing, it will put null
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv(s"$src/stocks.csv")

  // Parquet its the default extension in spark
  carsDF.write
    //    .format("parquet")  // redundant
    .mode(SaveMode.Overwrite)
    .parquet(s"$src/cars.parquet") // or use save

  // Text files
  spark.read.text(s"$src/sampleTextFile.txt").show

  // reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val username = "docker"
  val password = "docker"
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", username)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /*Exercise read the movie DF
  * - tab-separated values file
  * - snappy parquet
  * - table "public.movies" in the postgres DB
  * */

  val moviesDF = spark.read.json(s"$src/movies.json")
  //  TSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save(s"$src/movies.csv")
  // parquet
  moviesDF.write.save(s"$src/movies.parquet")
  // Postgres DB
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", username)
    .option("password", password)
    .option("dbtable","public.movies")
    .save()
}
