package app

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

object KafkaProducerApp extends App {

  private val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)

  private val topic = "employee"

  private def randomElement[T](input: List[T]): T = {
    val length = input.length
    val randomNumber = new Random().between(0, length - 1)
    input(randomNumber)
  }

  private val listOfLocations = List("Ocean", "Hill", "River", "Railway", "Forest", "City", "Desert", "Mountain")
  private val listOfOnlineStatus = List(true, false)
  private val listOfFollowers = (550 to 50000).toList

  private val isOnline = randomElement(listOfOnlineStatus)
  private val location = randomElement(listOfLocations)
  private val followerCount = randomElement(listOfFollowers)

  private val publishData = s"""{"firstname":"Manish","lastname":"Mishra", "location":$location, "online":$isOnline, "followers":$followerCount}"""
  private val record = new ProducerRecord(topic, "key", publishData)
  producer.send(record)

  producer.close()
}
