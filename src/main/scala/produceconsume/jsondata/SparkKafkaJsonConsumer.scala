package produceconsume.jsondata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkKafkaJsonConsumer extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("spark consumer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

  val topic = "json-data"

  val schema = StructType(Seq(
    StructField("first_name", StringType),
    StructField("last_name", StringType),
    StructField("location", StringType),
    StructField("online", BooleanType),
    StructField("followers", LongType),
    StructField("timestamp", TimestampType)
  ))

  val kafkaDf = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("group.id", "spark-json-consumer")
    .load()
    .selectExpr("CAST(value AS STRING) as userJson")
    .select(from_json(col("userJson"), schema).as("data"))
    .select("data.*")

  val filteredDf = kafkaDf.select("*").where(col("location") === "Oregon" || col("location") === "Indiana")

  //tumbling window
//  val windowedDf = filteredDf.withWatermark("timestamp", "10 minutes")
//    .groupBy(window(col("timestamp"), "10 minutes"), col("location"))
//    .agg(count("*").alias("location_count"))

  //sliding window
  val slidingWindow = filteredDf.withWatermark("timestamp", "10 minutes")
    .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"), col("location"))
    .agg(count("*").as("location_count"))

  val query = slidingWindow.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", false)
    //.option("path", "/home/knoldus/kafka-playground/UserInformation/src/main/outputFiles")
    //.option("checkpointLocation", "/home/knoldus/kafka-playground/UserInformation/src/main/checkpoints")
    //.partitionBy("location")
    .start()

  query.awaitTermination()

}
