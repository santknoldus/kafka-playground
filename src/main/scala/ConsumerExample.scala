import java.util.Properties
import org.apache.kafka.clients.consumer._
import scala.collection.JavaConverters._

object ConsumerExample extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test-consumer-group")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  val TOPIC = "test_topic"

  consumer.subscribe(java.util.Collections.singletonList(TOPIC))

  while (true) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      println(s"Received order details: ${record.value()}")
    }
  }

  consumer.close()

}
