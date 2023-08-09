import scala.util.Random
import java.util.Properties
import org.apache.kafka.clients.producer._
import play.api.libs.json._

object ProducerExample extends App {
  case class Order(id: Int, product: String, quantity: Int, price: Double)

  val props = new Properties()
  val producer = new KafkaProducer[String, String](props)

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val TOPIC = "test_topic"

  implicit val write: Writes[Order] = Json.writes[Order]

  for (i <- 1 to 10) {
    val order = generateRandomOrder(i)
    val orderJson = Json.toJson(order).toString()
    val record = new ProducerRecord[String, String](TOPIC, orderJson)
    producer.send(record)
  }

  producer.close()

  def generateRandomOrder(orderId: Int): Order = {
    val products = Array("ProductA", "ProductB", "ProductC")
    val randomProduct = products(Random.nextInt(products.length))
    val randomQuantity = Random.nextInt(10) + 1
    val randomPrice = Random.nextDouble() * 100
    Order(orderId, randomProduct, randomQuantity, randomPrice)
  }
}