Table of Contents
=================

   * [Table of Contents](#table-of-contents)
   * [Introduction](#introduction)
      * [Compile](#compile)
      * [Test](#test)
      * [Doc API](#doc-api)
      * [Assembly](#assembly)
      * [Deployment](#deployment)
   * [Usage](#usage)
   * [Under the hood](#under-the-hood)
   * [Credits](#credits)


# Introduction
Apache spark provides a very concise API for data frame to be written to RDBMS database. The usage for such API has been motivated by functional paradigm which states that function behavior should be idempotent. 
As a result, the default behavior of spark data frame write is all or nothing. 
```
jdbcDF.write
  .format("jdbc")
  .option("url", "jdbc:postgresql:dbserver")
  .option("dbtable", "schema.tablename")
  .option("user", "username")
  .option("password", "password")
  .save()
```
As mentioned in the link (https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) all options for writing to RDBMS follow the same pattern.

As this is the only behavior of spark, it remains (till the time of writing this blog) the same behavior of AWS Glue.
Depending upon the business scenario, there are some custom use case which requires bad records in a data frames to return back to the user. The user can then decide what to do with the failed records.

## Compile

This is a sbt based program so it needs sbt *(tested with 0.13.16 and higher)*.

The command to compile the program is `sbt compile`

## Test

The command to test the program is `sbt test`.
This test uses a dockerized postgres machine for it's itegration level testing.
Before stating test; start the docker instance of a postgres.

``` 
docker run --rm   --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 -v $HOME/docker/volumes/postgres:/var/lib/postgresql/data  postgres 
```


## Doc API

The command to create api doc is `sbt doc`. The api gets stored in target/scala<version>/api folder.

## Assembly

The command to create an assembly is `sbt assembly`


## Deployment

The deployment structure of the package looks similar to the example below.
 BulkCopy-assembly-1.0.jar. this fat jar can be imported by other application like AWS Glue or simple EMR / Spark applications.

# Usage
The partial data writer module thus takes advantage of Spark APIs to write chunk of data with in a `mapPartition`. On failure it reverts to single record insertion. In doing so, it keeps a tap on the failed records and sends it back as a data frame.

Import the jar by using your favorite method. 
``` spark-shell jars BulkCopy-assembly-1.0.jar```

The simple API to write to RDBMS is 
```
val failedRecords = CustomJDBCUtils.write(database = "postgres", table = "default.student", dataframe = df, optMapJava = opt.asJava, cutOffPercentage = 90)
```
`optMapJava` is the JDBCOption in a java map. The decision to use a java map as opposed to a scala map is driven by the fact that java collection objects are easier to operate via python.
# Under the hood
At the heart of the `write` function the code invokes a `writeToDB(iterator[Row]..)` function with in each `mapPartition`.
It then calls an action on it.
The trick used so that the data does not get written multiple times is to persist the data frame before calling the action.

*There exist a case where the executor will die destroying the data frame partition and kick off the calculation again.
 We can use `SaveMode.MEMORY_AND_DISK2` but that is the extent of the protection we can give to the corner case.*

# Credits
This open source contribution is successful because of the following heroes at SLF.

* Abhay Raman
* Winston Wu
* Upal Hossain
* Seth Chalmers
* Manas Kar