package com.slf

import java.sql.{Connection, PreparedStatement, SQLException}
import java.util.Locale

import org.apache.log4j._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * The object provides to write partial output to database.
  * Please go through `write` function to checkout the heart of the code.
  */
object CustomJDBCUtils extends Serializable {
  @transient val logger = Logger.getLogger(getClass)

  type JDBCValueSetter =
    (PreparedStatement, Row, Int) =>
      Unit

  /**
    * @param database   The database where the dataframe will be written
    * @param dbtable    The table where the dataframe will be written. `format = schema.table`
    * @param df         the dataframe that will be written
    * @param optMapJava connection string
    *                   `(e.g.  {url=jdbc:postgresql://localhost:5432/postgres, driver=org.postgresql.Driver, dbtable=student, user=postgres, password=docker}))``
    *                   @param cutOffPercentage The percentage above which the process shall not continue.
    *                   @param repartition defaults to false.
    *                   @return a dataframe of the failed entries
    *                   Currently supports sql server and postgres if driver is not provided.
    **/
  def write(database: String, dbtable: String, df: DataFrame, optMapJava: java.util.Map[String, String], cutOffPercentage: Int): DataFrame = {
    df.printSchema()
    println(df.first())
    val jm
    = if (!optMapJava.asScala.toMap.contains("driver")) {
      optMapJava.asScala.toMap.get("vendor") match {
        case Some("sqlserver") => optMapJava.asScala.toMap.updated("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        case Some("postgresql") => optMapJava.asScala.toMap.updated("driver", "org.postgresql.Driver")
        case _ => throw new Exception("vendor not supported")
      }
    }.updated("database", database).updated("dbtable", dbtable)
    else optMapJava.asScala.toMap.updated("database", database).updated("dbtable", dbtable)

    val schema = df.schema
    implicit val en = RowEncoder(schema)
    val cutOffNumber = df.count() //TODO: The cut off part does not work as expected.
    val partitions = cutOffNumber / 10000.0 + 3
    val failed = df.repartition(partitions.toInt).mapPartitions { iterator =>
      Class.forName(jm.getOrElse("driver", ""))
      val jdbcOption = new JDBCOptions(jm)
      val connection = JdbcUtils.createConnectionFactory(jdbcOption)
      val conn = connection()
      val dialect = JdbcDialects.get(jdbcOption.url)
      val isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED
      print(s"Supports transaction ${conn.getMetaData.supportsTransactions()}\n")
      val insertStmt = JdbcUtils.getInsertStatement(jm.getOrElse("dbtable", "SCHEMA.TABLE NOT PROVIDED"), schema, JdbcUtils.getSchemaOption(conn, jdbcOption), false, dialect)
      val (first, second) = iterator.duplicate
      Try {
        JdbcUtils.savePartition(connection,
          dbtable,
          first,
          schema,
          insertStmt,
          10000,
          dialect,
          isolationLevel
        )
      } match {
        case scala.util.Failure(e: SQLException) =>
          println(s"Failed with $e")
          if(e.getMessage.contains("getNextException")) {
            println(e.getNextException)
            println(e.getNextException.getStackTraceString)
          }
          writeToDB(second, schema, jm).toIterator
        case _ => Iterator.empty
      }
    }
    failed.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(s"Number of failed records are ${failed.count}") //Calling an action so that the mapPartition kicks in; in case user forgets.
    failed
  }

  def writeToDB(iterator: Iterator[Row], rddSchema: StructType, jm: Map[String, String]): List[Row] = {


    val failedRecords = scala.collection.mutable.ListBuffer.empty[Row]


    val jdbcOption = new JDBCOptions(jm)
    val connection = JdbcUtils.createConnectionFactory(jdbcOption)
    val conn = connection()
    val tableSchema = JdbcUtils.getSchemaOption(conn, jdbcOption)
    val dialect = JdbcDialects.get(jdbcOption.url)
    val insertStmt = JdbcUtils.getInsertStatement(jm.getOrElse("dbtable", "SCHEMA.TABLE NOT PROVIDED"), rddSchema, tableSchema, false, dialect)
    val stmt = conn.prepareStatement(insertStmt)
    val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
    val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
    val numFields = rddSchema.fields.length
    conn.setAutoCommit(true)
    //TODO  conn.setTransactionIsolation(FIXME)

    Try {
      while (iterator.hasNext) {
        val row = iterator.next()
        var i = 0
        while (i < numFields) {
          if (row.isNullAt(i)) {
            stmt.setNull(i + 1, nullTypes(i))
          } else {
            setters(i)(stmt, row, i)
          }
          i = i + 1
        }
        Try {
          stmt.executeUpdate()
        } match {
          case Success(v) => v
          case Failure(e) =>
            failedRecords.+=(row)
        }
      }
    } match {
      case Failure(e) =>
      case Success(value) => value
    }
    failedRecords.toList
  }

  def makeSetter(
                  conn: Connection,
                  dialect: JdbcDialect,
                  dataType: DataType): JDBCValueSetter
  = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase(Locale.ROOT).split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType

  = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.catalogString}"))
  }

}
