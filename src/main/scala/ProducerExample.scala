import scala.util.Random

object ProducerExample extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test_topic"

  val random  = new Random()

  val names = List("Sant", "Tushar", "akhil", "Ayush")

  for(i<- 1 to 10){
    val randomId = random.nextInt(10)
    val randomName = names(random.nextInt(names.size - 1))
    val jsonMessage = s"""{"id": $randomId, "name": $randomName}"""
    val record = new ProducerRecord[String, String](TOPIC, jsonMessage)
    producer.send(record)
  }

  producer.close()
}