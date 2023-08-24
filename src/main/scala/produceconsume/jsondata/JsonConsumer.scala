package produceconsume.jsondata

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.Properties
object JsonConsumer extends App {
  val topic = "json-data"

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "json-consumer-group")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(java.util.Collections.singletonList(topic))

  while (true) {
    val records = consumer.poll(java.time.Duration.ofMillis(100))
    import scala.jdk.CollectionConverters._
    for (record <- records.asScala) {
      val userJson = record.value()
      println(s"Received Json User Record: $userJson")
    }
  }
  consumer.close()
}
