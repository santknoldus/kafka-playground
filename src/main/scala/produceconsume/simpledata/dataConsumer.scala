package produceconsume.simpledata

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import java.util.Properties

object KafkaConsumerApp extends App {
  val topic = "topic-1"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(java.util.Collections.singletonList(topic))

  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(java.time.Duration.ofMillis(100))
    import scala.jdk.CollectionConverters._
    for (record: ConsumerRecord[String, String] <- records.asScala) {
      println(s"Received Message: key = ${record.key()}, value = ${record.value()}")
    }
  }
  consumer.close()
}

