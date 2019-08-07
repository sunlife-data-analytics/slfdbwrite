package com.slf


import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import collection.JavaConverters._

case class Student(name: String, roll: Int)

//TODO write to db using simple method.
//TODO create transaction around it.
//TODO expose via pyspark using https://stackoverflow.com/questions/36023860/how-to-use-a-scala-class-inside-pyspark
//TODO jdbcConf = glueContext.extract_jdbc_conf("C360_SL_STG")

class IngestionSpec extends FunSuite with DataFrameSuiteBase with BeforeAndAfterAll {
  test("SQL insertion should be successful") {
    val df = spark.createDataFrame(Student("manas", 1) :: Student("manas1", 2) :: Nil)

    val url = "jdbc:postgresql://localhost:5432/postgres"

    val opt = Map("url" -> url, "vendor" -> "postgresql", "password" -> "docker", "user" -> "postgres", "dbtable" -> "student")

    val jdbcOption = new JDBCOptions(opt)

    val connection = JdbcUtils.createConnectionFactory(jdbcOption)
    val conn = connection()
    conn.prepareStatement("drop table student").executeUpdate()
    conn.prepareStatement("create table if not exists student(name text primary key, roll int)").executeUpdate()
    conn.prepareStatement("delete from student").executeUpdate()
    conn.prepareStatement("insert into student values ('manas', 1)").executeUpdate()

    val failedRecords = CustomJDBCUtils.write("postgres", "student", df, opt.asJava, 90)
    failedRecords.foreach { record => println(s"failed $record") }
    spark.read.jdbc(url, "student", jdbcOption.asConnectionProperties).show()
  }
}