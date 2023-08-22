package com.knoldus
package Subscriber

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Windowing extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Windowing")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

  import spark.implicits._

  val kafkaOptions = Map(
    "kafka.bootstrap.servers" -> "localhost:9092",
    "subscribe" -> "quickstart-json",
    "group.id" -> "json-consumer-group",
    "startingOffsets" -> "latest",
    "maxOffsetsPerTrigger" -> "10"
  )

  // Define the schema for JSON data
  val jsonSchema = StructType(Seq(
    StructField("first_name", StringType),
    StructField("last_name", StringType),
    StructField("location", StringType),
    StructField("online", BooleanType),
    StructField("followers", IntegerType),
    StructField("timestamp", TimestampType)
  ))

  // Read data from Kafka topic into DataFrame
  val kafkaDataFrame = spark.readStream
    .format("kafka")
    .options(kafkaOptions)
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json($"json", jsonSchema).as("data"))
    .select("data.*")

  // Processing DataFrame
  val filteringOnDF = kafkaDataFrame.select("*").where(col("location") === "Dubai" || col("location") === "India")

  // Tumbling Window
  val tumblingWindowDF =
    filteringOnDF
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "10 minutes"), col("location"))
      .agg(count("*").as("occurrence_count"))

  // Sliding Window
  val slidingWindowDF =
    filteringOnDF
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"), col("location"))
      .agg(count("*").as("occurrence_count"))

  //  // Start the streaming query
  val query = slidingWindowDF.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()

  // Wait for the query to terminate
  query.awaitTermination()
}
