import scala.util.Random
import java.util.{Calendar, Properties}
import org.apache.kafka.clients.producer._
import play.api.libs.json._

object ProducerExample extends App {
  case class Order(id: Int, product: String, quantity: Int, price: Double, time: String)

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "sample_topic"
  implicit val write: Writes[Order] = Json.writes[Order]

  for (i <- 1 to 10) {
    val order = generateRandomOrder(i)
    val orderJson = Json.toJson(order).toString()
    //val data = s"""{"orderId":${order.id},"product":${order.product},"quantity":${order.quantity},"price":${order.price}}"""
    val record = new ProducerRecord[String, String](TOPIC, orderJson)
    producer.send(record)
  }

  producer.close()

  def generateRandomOrder(orderId: Int): Order = {
    val products = Array("ProductA", "ProductB", "ProductC")
    val randomProduct = products(Random.nextInt(products.length))
    val randomQuantity = Random.nextInt(10) + 1
    val randomPrice = Random.nextDouble() * 100

    val time = Calendar.getInstance().getTime.getHours.toString + ":" + (Calendar.getInstance().getTime.getMinutes - new Random().between(10, 15))
    Order(orderId, randomProduct, randomQuantity, randomPrice, time)
  }
}