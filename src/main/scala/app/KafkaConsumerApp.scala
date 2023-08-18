package app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType}

import scala.concurrent.duration.DurationInt

object KafkaConsumerApp extends App {

  private val spark = SparkSession.builder()
    .appName("kafka-consumer")
    .master("local[3]")
    .getOrCreate()

  private val data = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "employee")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "9")
    .load

  private val schema = new StructType()
    .add("id", IntegerType)
    .add("firstname", StringType)
    .add("lastname", StringType)
    .add("location", StringType)
    .add("online", BooleanType)
    .add("followers", IntegerType)

  private val dataframe = data.select(from_json(col("value").cast(StringType), schema).as("data"))
    .select("data.*")

  private val countFollowersByFirstName =
    dataframe
      .filter("followers >= 1000")

  /* This code snippet print the output on the console */
  /*countFollowersByFirstName
    .writeStream
    .format("console")
    .partitionBy("firstname")
    .outputMode("complete")
    .start()
    .awaitTermination()*/

  countFollowersByFirstName
    .writeStream
    .partitionBy("firstname")
    .format("json")
    .option("path", "/home/knoldus/Desktop/KUP/DATA Bricks/kafka-demo/kafka-playground/src/main/resources/DataAsPerName")
    .option("checkPointLocation", "/home/knoldus/Desktop/KUP/DATA Bricks/kafka-demo/kafka-playground/src/main/resources/Checkpoint")
    .option("mode", "overwrite")
    .trigger(Trigger.ProcessingTime(10.seconds))
    .start()
    .awaitTermination()


}
