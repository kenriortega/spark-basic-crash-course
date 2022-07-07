package part2df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object JoinsOp extends App {

  /*Joins
  * Combine data from multiples DataFrames
  * one (or more) column from table 1 (left) is compared with one (or more) column from table 2 (right
  * if the condition passes, rows are combined
  * non-matching rows are discared
  * wide transformation (read: expensive)
  * */
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarDf = spark.read
    .option("inferSchema", "true")
    .json(s"$src/guitars.json")

  val guitaristsDf = spark.read
    .option("inferSchema", "true")
    .json(s"$src/guitarPlayers.json")

  val bandsDf = spark.read
    .option("inferSchema", "true")
    .json(s"$src/bands.json")

  // inner joins (most used)
  val joinCondition = guitaristsDf.col("band") === bandsDf.col("id")
  val guitaristBandsDF = guitaristsDf.join(
    bandsDf,
    joinCondition,
    "inner"
  )
  // outer joins
  // left outer = everything in the inner join + all the rows in the left table
  // with null in where the data is missing
  guitaristsDf.join(bandsDf, joinCondition, "left_outer")
  // right outer = everything in the inner join + all the rows in the right table
  // with null in where the data is missing
  guitaristsDf.join(bandsDf, joinCondition, "right_outer")
  // outer joins = in the inner join + all the rows in the BOTH table
  // with null in where the data is missing
  guitaristsDf.join(bandsDf, joinCondition, "outer")
  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDf.join(bandsDf, joinCondition, "left_semi")
  // anti_joins = everything in the left DF for which there is No row in the right DF satisfying the condition
  guitaristsDf.join(bandsDf, joinCondition, "left_anti")

  //  guitaristBandsDF.select("id", "band").show() // Crash ambiguous id
  // option 1 - rename the column on which we are join
  guitaristsDf.join(bandsDf.withColumnRenamed("id", "band"), "band")
  // option 2
  guitaristBandsDF.drop(bandsDf.col("id"))
  // option 3
  val bandsModDF = bandsDf.withColumnRenamed("id", "bandId")
  guitaristsDf.join(bandsModDF, guitaristsDf.col("band") === bandsModDF.col("bandId"))
  // using complex types
  guitaristsDf.join(
    guitarDf.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars,guitarId)")
  )

  /*Exercise
  * - show all employees and their max salary
  * - show all employees who were never managers
  * - find the job titles of the best paid 10 employees in the company*/

  // reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val username = "docker"
  val password = "docker"
  val opts = Map(
    "driver" -> driver,
    "url" -> url,
    "user" -> username,
    "password" -> password,
  )

  def readTable(tableName: String, optionsMap: Map[String, String] = opts) =
    spark.read
      .format("jdbc")
      .options(optionsMap)
      .option("dbtable", s"public.$tableName")
      .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagerDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no")
    .agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // 2
  val empNeverManagersDF = employeesDF.join(
    deptManagerDF,
    employeesDF.col("emp_no") === deptManagerDF.col("emp_no"),
    "left_anti",
  )
  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no","title")
    .agg(max("to_date"))
  val bestPaidEmpDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmpDF.join(mostRecentJobTitlesDF,"emp_no")

  bestPaidJobsDF.show
}
