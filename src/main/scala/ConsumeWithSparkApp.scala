import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object ConsumeWithSparkApp extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("ConsumeWithSparkApp")
    .master("local[1]")
    .getOrCreate()

  val dataframe = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sample_topic")
    .option("startingOffsets", "earliest")
   // .option("maxOffsetsPerTrigger", "9")
    .load

  //dataframe.printSchema()
  val orderStringDF = dataframe.selectExpr("CAST(value AS STRING)")

  val schema = new StructType()
    .add("id", IntegerType)
    .add("product", StringType)
    .add("quantity", IntegerType)
    .add("price", DoubleType)
    .add("time", StringType)

  val orderDF = orderStringDF.select(from_json(col("value"), schema).as("data")).select("data.*")

  val filteredData = orderDF.filter(col("product") === "ProductA")

  val tumblingWindowEvent =
    orderDF
      .groupBy(window(col("time"), "10 minutes"))
      .count()

//consuming streaming data and write it on console
  filteredData.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()

//consuming streaming data with tumbling window and write it on console
  tumblingWindowEvent.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()

//saving the consumed data on local
  filteredData
    .writeStream
    .partitionBy("id")
    .format("json")
    .option("path", "/home/knoldus/learning/learningSpark/kafka-client/SavedData")
    .option("checkPointLocation", "/home/knoldus/learning/learningSpark/kafka-client/checkpoint")
    .option("mode", "overwrite")
    .start()
    .awaitTermination()

}