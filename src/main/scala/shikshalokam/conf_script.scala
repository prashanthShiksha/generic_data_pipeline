package shikshalokam

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties
import scala.io.Source
import scala.util.{Failure, Success, Try}


object conf_script {

  // Set log level to WARN to reduce unnecessary output
  Logger.getLogger("org").setLevel(Level.WARN)

  // Initialize Spark session
  val spark: SparkSession = SparkSession.builder
    .appName("process_and_write_to_postgres")
    .master("local[*]")
    .getOrCreate()

  // Read PostgreSQL config from external file
  val configProperties: Properties = new Properties()
  configProperties.load(getClass.getResourceAsStream("/application.properties"))

  val dbUser: String = configProperties.getProperty("db.user")
  val dbPassword: String = configProperties.getProperty("db.password")


  println(dbUser)
  println(dbPassword)

  // Read data from PostgreSQL into a DataFrame
  val pgConnectionProperties = new java.util.Properties()
  pgConnectionProperties.put("user", dbUser)
  pgConnectionProperties.put("password", dbPassword)

  def processFetchID(fetchID: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties): List[Int] = {

    try {
      val query = fetchID("query").str
      val databaseName = fetchID("database_name").str
      val id = fetchID("id").str
      val url = s"jdbc:postgresql://localhost:5432/$databaseName"

      //Print or process fetchID information
      println(s"Query: $query, Database: $databaseName, ID: $id, URL:$url")

      // Spark process to read data from JDBC source
      val df: DataFrame = spark.read.jdbc(url, s"($query)", pgConnectionProperties)
      df.show()
      // Extract values from the DataFrame
      val idList: List[Int] = df.select(id).rdd.map(r => r.getInt(0)).collect().toList
      idList
    } catch {
      case e: Exception =>
        println(s"Error processing Spark DataFrame: ${e.getMessage}")
        List.empty[Int]
    }
  }

  def processSingleProcess(singleProcessData: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, value: Int): DataFrame = {

    try {
      val input = singleProcessData("input").str
      val query = singleProcessData("query").str
      val databaseName = singleProcessData("database_name").str
      val url = s"jdbc:postgresql://localhost:5432/$databaseName"

      // Replace placeholders in the query
      val substitutedQuery = query.replace("${id}", value.toString)

      // Process single input data
      println(s"Single Process: Input: $input, Query: $query, Database: $databaseName,URL:$url,substitutedQuery: $substitutedQuery")

      // Spark process to read data from JDBC source
      var df: DataFrame = spark.read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties)
      df = df.withColumn(input, lit(value))
      df.show()
      df

    } catch {
      case e: Exception =>
        println(s"Error processing Spark DataFrame: ${e.getMessage}")
        spark.emptyDataFrame
    }
  }

  def processMultipleProcess(multipleProcessData: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, idList: List[Int], value: Int): DataFrame = {
    // Extract configuration values
    val input = multipleProcessData("input").str
    val query = multipleProcessData("query").str
    val databaseName = multipleProcessData("database_name").str
    val groupBy = multipleProcessData("groupBy").str
    val agg = multipleProcessData("agg").str
    val agg_on = multipleProcessData("agg_on").str
    val join_on = multipleProcessData("join_on").str
    val url = s"jdbc:postgresql://localhost:5432/$databaseName"
    val rename = multipleProcessData("rename").str

    // Print input details
    println(s"Multiple Process: Input: $input, Query: $query, Database: $databaseName, GroupBy: $groupBy, Aggregate: $agg, Aggregate_on: $agg_on")

    // Wrap the main logic in a Try block for error handling
    val result: DataFrame = Try {
      // Collect DataFrames in a list
      val dfs: List[DataFrame] = idList.flatMap { ids =>
        // Replace placeholders in the query
        val substitutedQuery = query.replace("${id}", ids.toString)
        println(s"substitutedQuery: $substitutedQuery")

        // Read data from PostgreSQL, replace null values with 0, and cast the 'response' column to IntegerType
        val df: DataFrame = spark
          .read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties)
          .na.fill(0)
          .withColumn(agg_on, col(agg_on).cast("integer"))

        if (df.count == 0) {
          // Create a new DataFrame with a single column "response" and value 0
          val newSchema = StructType(Array(StructField(agg_on, IntegerType, true:Boolean)))
          val newData = Seq(Row(0))
          val newRdd = spark.sparkContext.parallelize(newData)
          val newDf = spark.createDataFrame(newRdd, newSchema)
          List(newDf)
        } else {
          println(s"printing df :")
          df.show()
          List(df) // Wrap the DataFrame in a List to make it TraversableOnce
        }
      }

      // Union all DataFrames, including those with zero values
      var resultDF: DataFrame = dfs.reduceOption(_ unionAll _).getOrElse(spark.emptyDataFrame)

      // Perform grouping and aggregation, and add a new column 'join_on' with a constant value
      resultDF = resultDF.withColumn(join_on, lit(value))
      val groupedResultDF = resultDF.groupBy(col(join_on)).agg(sum(agg_on).alias(rename))
      groupedResultDF.show()
      groupedResultDF // Return the grouped DataFrame
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"Error processing Spark DataFrame: ${exception.getMessage}")
        spark.emptyDataFrame // Return an empty DataFrame or handle the exception as needed
    }
    result
  }

//  def processMultipleProcess(multipleProcessData: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, idList: List[Int], value: Int): DataFrame = {
//    // Extract configuration values
//    val input = multipleProcessData("input").str
//    val query = multipleProcessData("query").str
//    val databaseName = multipleProcessData("database_name").str
//    val dataframeName = multipleProcessData("dataframe_name").str
//    val groupBy = multipleProcessData("groupBy").str
//    val agg = multipleProcessData("agg").str
//    val agg_on = multipleProcessData("agg_on").str
//    val join_on = multipleProcessData("join_on").str
//    val url = s"jdbc:postgresql://localhost:5432/$databaseName"
//    val rename = multipleProcessData("rename").str
//
//    // Print input details
//    println(s"Multiple Process: Input: $input, Query: $query, Database: $databaseName, DataFrame: $dataframeName, GroupBy: $groupBy, Aggregate: $agg, Aggregate_on: $agg_on")
//
//    // Wrap the main logic in a Try block for error handling
//    val result: DataFrame = Try {
//      // Collect DataFrames in a list
//      val dfs: List[DataFrame] = idList.flatMap { ids =>
//
//        // Replace placeholders in the query
//        val substitutedQuery = query.replace("${id}", ids.toString)
//        println(s"substitutedQuery: $substitutedQuery")
//
//        // Read data from PostgreSQL and cast the 'response' column to IntegerType
//        val df: DataFrame = spark
//          .read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties)
//          .withColumn("response", col("response").cast("integer"))
//
//        if (df.filter(col("response").isNotNull).count == 0) {
//          // Print a message indicating that no non-null data is available for the given session_id
//          println(s"No non-null data found for session_id: $ids")
//
//          // Create a DataFrame with zero values and the expected schema
//          val emptyDf = spark.createDataFrame(Seq((0, 0))).toDF("response", "other_column")
//          println(emptyDf)
//          List(emptyDf)
//        } else {
//          println(s"printing df :")
//          df.show()
//          List(df) // Wrap the DataFrame in a List to make it TraversableOnce
//        }
//      }
//
//      // Union all DataFrames, including those with zero values
//      var resultDF: DataFrame = dfs.reduceOption(_ unionAll _).getOrElse(spark.emptyDataFrame)
//
//      // Check if the result DataFrame is empty
//      if (resultDF.count == 0) {
//        // If the result DataFrame is empty, create a DataFrame with zero values and the expected schema
//        spark.createDataFrame(Seq((0, 0))).toDF("response", "other_column")
//      } else {
//        // Perform grouping and aggregation, and add a new column 'join_on' with a constant value
//        resultDF = resultDF.withColumn(join_on, lit(value))
//        val groupedResultDF = resultDF.groupBy(join_on).agg(expr(s"$agg(coalesce($agg_on, 0))").alias(rename))
//        groupedResultDF.show()
//        groupedResultDF // Return the grouped DataFrame
//      }
//    } match {
//      case Success(result) => result
//      case Failure(exception) =>
//        println(s"Error processing Spark DataFrame: ${exception.getMessage}")
//        spark.emptyDataFrame // Return an empty DataFrame or handle the exception as needed
//    }
//    result
//  }

  def processFetchID_with_InputID(FetchID_with_InputID: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, value: Int): List[Int] = {
    try {
      val query = FetchID_with_InputID("query").str
      val databaseName = FetchID_with_InputID("database_name").str
      val url = s"jdbc:postgresql://localhost:5432/$databaseName"
      val select_id = FetchID_with_InputID("id").str

      // Replace placeholders in the query
      val substitutedQuery = query.replace("${id}", value.toString)

      //Print or process fetchID information
      println(s"Query: $query, Database: $databaseName, URL:$url , SubstitutedQuery:$substitutedQuery")

      // Spark process to read data from JDBC source
      val df: DataFrame = spark.read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties)
      df.show()

      // Extract values from the DataFrame
      val idList: List[Int] = df.select(select_id).rdd.map(r => r.getInt(0)).collect().toList
      idList
    } catch {
      case e: Exception =>
        println(s"Error processing Spark DataFrame: ${e.getMessage}")
        List.empty[Int]
    }
  }

  def single_process_for_user_data(singleProcessData: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, value: Int): DataFrame = {
    try {
      val input = singleProcessData("input").str
      val query = singleProcessData("query").str
      val databaseName = singleProcessData("database_name").str
      val url = s"jdbc:postgresql://localhost:5432/$databaseName"

      // Replace placeholders in the query
      val substitutedQuery = query.replace("${id}", value.toString)

      // Process single input data
      println(s"Single Process: Input: $input, Query: $query, Database: $databaseName,URL:$url,substitutedQuery: $substitutedQuery")

      // Spark process to read data from JDBC source
      var df: DataFrame = spark.read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties)
      df = df.withColumn(input, lit(value))
      df.show()
      df

    } catch {
      case e: Exception =>
        println(s"Error processing Spark DataFrame: ${e.getMessage}")
        spark.emptyDataFrame
    }
  }





  def processChild(child: ujson.Arr, spark: SparkSession, pgConnectionProperties: Properties, value: Int): DataFrame = {
    var child_len: Int = child.arr.length
    var joinedDF: DataFrame = null
    println(child_len)
    println(child)
    for (j <- 0 until child_len) {
      if (j == 0) {
        println(child(0))
        if (child(0).obj.contains("single_process")) {
          val df = processSingleProcess(child(0)("single_process"), spark, pgConnectionProperties, value)
          joinedDF = df
        }
      } else {
        println(child(j))
        if (child(j).obj.contains("single_process")) {
          val df2 = processSingleProcess(child(j)("single_process"), spark, pgConnectionProperties, value)
          val join_on = child(j)("single_process")("join_on").str
          val join_type = child(j)("single_process")("join_type").str
          joinedDF = df2.join(joinedDF, Seq(join_on), join_type)
          joinedDF.show()
        } else if (child(j).obj.contains("fetchID_with_InputID")) {
          val idList2: List[Int] = processFetchID_with_InputID(child(j)("fetchID_with_InputID"), spark, pgConnectionProperties, value)
          val join_on2 = child(j)("fetchID_with_InputID")("join_on").str
          val join_type2 = child(j)("fetchID_with_InputID")("join_type").str
          println(idList2)
          if (child(j).obj.contains("child")) {
            val child_data2 = child(j)("child").arr
            println(child_data2)
            var df3 = processChildWithMultipleProcess(child_data2, spark, pgConnectionProperties, idList2,value)
            joinedDF = df3.join(joinedDF, Seq(join_on2), join_type2)
          }
        }
      }
    }
    joinedDF
  }

  def processChildWithMultipleProcess(child: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, idList2: List[Int],value:Int): DataFrame = {
    val childLen = child.arr.length
    println(s"childLen:$childLen")
    var joinedDF: DataFrame = null

    for (j <- 0 until childLen) {
      if (j == 0) {
        if (child(j).obj.contains("multiple_process")) {
          var df = processMultipleProcess(child(j)("multiple_process"), spark, pgConnectionProperties, idList2, value)
          joinedDF = df
        }
      } else if (child(j).obj.contains("multiple_process")) {
        val df = processMultipleProcess(child(j)("multiple_process"), spark, pgConnectionProperties, idList2,value)
        val join_on = child(j)("multiple_process")("join_on").str
        val join_type = child(j)("multiple_process")("join_type").str
        joinedDF = df.join(joinedDF, Seq(join_on), join_type)
      }
    }

    joinedDF
  }

//  def processJson(json: ujson.Value):DataFrame = {
//    // Check if the JSON object has "fetchID" key
//    if (json.obj.contains("fetchID")) {
//      val fetchIdData = json("fetchID")
//      var idList = processFetchID(fetchIdData, spark: SparkSession, pgConnectionProperties: Properties)
//      if (json.obj.contains("child")) {
//        for (value <- idList) {
//          val childData = json("child")
//          val df: DataFrame = processChild(childData.arr, spark: SparkSession, pgConnectionProperties: Properties, value)
//        }
//        df
//      }
//      else {
//        spark.emptyDataFrame
//      }
//    }
//    else {
//
//    }
//  }
  def processJson(json: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties): DataFrame = {
    // Check if the JSON object has "fetchID" key
    if (json.obj.contains("fetchID")) {
      val fetchIdData = json("fetchID")
      val idList = processFetchID(fetchIdData, spark, pgConnectionProperties)

      var df: DataFrame = spark.emptyDataFrame  // Move df declaration outside the loop

      if (json.obj.contains("child")) {
        for (value <- idList) {
          val childData = json("child")
          // Assuming processChild returns a DataFrame
          df = processChild(childData.arr, spark, pgConnectionProperties, value)
        }
      }

      df  // Return df outside the if statement
    } else {
      // If there is no "fetchID" key, you might want to handle this case accordingly.
      // For now, return an empty DataFrame.
      spark.emptyDataFrame
    }
  }




  def main(args: Array[String]): Unit = {
    val json_file_path = "/home/user1/Documents/elevate/MentorED/conf2_data_pipeline/src/main/resources/mentee_report.json"
    val jsonString = Source.fromFile(json_file_path).mkString
    val json: ujson.Value = ujson.read(jsonString)

    // Process the JSON structure
    val dataArray = json.arr
    println(dataArray)
    dataArray.foreach { queryData =>
      val df : DataFrame = processJson(queryData,spark:SparkSession,pgConnectionProperties:Properties)
    }
  }
}
