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

  private val listOfFirstName = List("Manish", "Sant", "Pradyuman", "Mohika", "Akhil", "Jitendar", "Tushar", "Ajit")
  private val listOfLastName = List("Mishra", "Kumar", "Awana", "Sharma", "Dhiman", "Tiwari")
  private val listOfLocations = List("Ocean", "Hill", "River", "Railway", "Forest", "City", "Desert", "Mountain")
  private val listOfOnlineStatus = List(true, false)
  private val listOfFollowers = (550 to 50000).toList

  for {
    i <- 1 to 10
    firstname = randomElement(listOfFirstName)
    lastname = randomElement(listOfLastName)
    isOnline = randomElement(listOfOnlineStatus)
    location = randomElement(listOfLocations)
    followerCount = randomElement(listOfFollowers)

    publishData =
      s"""{
         |"id":$i,
         |"firstname":"$firstname",
         |"lastname":"$lastname",
         |"location":"$location",
         |"online":$isOnline,
         |"followers":$followerCount
         |}"""
        .stripMargin
    record = new ProducerRecord(topic, "key", publishData)
  } yield producer.send(record)

  producer.close()
}
