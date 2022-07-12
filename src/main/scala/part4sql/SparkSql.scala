package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object SparkSql extends App {
  val src = "src/main/resources/data"

  val spark = SparkSession.builder()
    .appName("SparkSql")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/cars.json")

  carsDF.select(col("Origin") === "USA")

  // use Spark Sql
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )
  americanCarsDF.show

  // we can run any sql statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val dbDF = spark.sql("show databases")

  // transfer tables from a DB to spark tables
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

  //
  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach {
    tableName =>
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)
      if (shouldWriteToWarehouse) {
        tableDF.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      }
  }
  //
    transferTables(List(
      "employees",
      "departments",
      "titles",
      "dept_emp",
      "salaries",
      "dept_manager"
    )
    )
  //
  //  // read DF from warehouse
  //  val employeesDF2 = spark.read.table("employees")

  /*
  * Exercises
  *
  * 1- Read the movies DF and store it as a Spark table in the rtjvm databse
  * 2- Count how many employees were hired in Jan 1 2000 and Jan 1 2001
  * 3- Show the average salaries for the employees hired in between those dates, grouped by department
  * 4- Show the name of the best-paying department for employees hired in between those dates.
  * */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$src/movies.json")

  //  moviesDF.write
  //    .mode(SaveMode.Overwrite)
  //    .saveAsTable("movies")

  // 2
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin).show

  // 3
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin).show

  // 4
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin).show()
}
