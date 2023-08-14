package com.knoldus
package Subscriber

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object SparkConsumer extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("KafkaJsonConsumer")
    .master("local[*]")
    .getOrCreate()

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
    StructField("followers", IntegerType)
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
  val filteringOnDF = kafkaDataFrame.filter("followers > 500")

//  // Start the streaming query - Per First Name
//  val query = filteringOnDF.writeStream
//    .outputMode("append")
//    .format("csv")
//    .option("path", "/home/knoldus/IdeaProjects/kafka-playground/src/main/resources/perFirstName")
//    .option("checkpointLocation", "/home/knoldus/IdeaProjects/kafka-playground/src/main/checkpoints/perFirstName")
//    .option("mode", "overwrite")
//    .trigger(Trigger.ProcessingTime("10 seconds"))
//    .partitionBy("first_name")
//    .start()

//  // Start the streaming query - Per Location
  val query = filteringOnDF.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "/home/knoldus/IdeaProjects/kafka-playground/src/main/resources/perLocation")
    .option("checkpointLocation", "/home/knoldus/IdeaProjects/kafka-playground/src/main/checkpoints/perLocation")
    .option("mode", "overwrite")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .partitionBy("location")
    .start()

  // Wait for the query to terminate
  query.awaitTermination()
}
