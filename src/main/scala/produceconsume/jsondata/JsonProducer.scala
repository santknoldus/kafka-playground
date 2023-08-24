package produceconsume.jsondata

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.util.Random

object JsonProducer extends App {
  //kafka topic to which we send messages to
  val topic = "json-data"

  //to set properties for producer
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  //for variable user data
  val locationList = List("Ocean", "Oregon", "Indiana", "Washington", "NewYork")
  val onlineStatus = List(true, false)
  val random = new Random()

  for (_ <- 1 to 5) {
    val firstName = "Sammy"
    val lastName = "Shark"
    val location = locationList(random.nextInt(locationList.size))
    val online = onlineStatus(random.nextInt(onlineStatus.size))
    val followers = Random.between(500, 50000)
    val timestamp = System.currentTimeMillis()

    //data to json string
    val userJson =
      s""" {
         | "first_name": "${firstName}" ,
         | "last_name": "$lastName",
         | "location": "$location",
         | "online": $online,
         | "followers": $followers,
         | "timestamp": $timestamp
         |}""".stripMargin

    //kafka record to send json data
    val record = new ProducerRecord[String, String](topic, userJson)
    producer.send(record)
  }
  producer.close()
}
