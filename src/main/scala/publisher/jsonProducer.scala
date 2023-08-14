package com.knoldus
package publisher

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.util.Random

object jsonProducer extends App {
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val topic = "quickstart-json"

  // JSON Data ----------------------------------------------------------------------------------

  private val firstNames = List("Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Helen", "Ivy", "Jack")
  private val lastNames = List("Rajput", "Mishra", "Narayan", "Rastogi", "Kumar", "Gupta", "Rana", "Singh")
  private val locations = List("India", "Vietnam", "Spain", "Dubai", "London", "Barcelona", "Portugal")
  private val onlineStatus = List(true, false)

  // Generating Data and Sending JSON String as data ----------------------------------------------------------------------------

  for (_ <- 1 to 10) {
    val firstAName = firstNames(Random.nextInt(firstNames.length))
    val lastName = lastNames(Random.nextInt(lastNames.length))
    val location = locations(Random.nextInt(locations.length))
    val online = onlineStatus(Random.nextInt(onlineStatus.length))
    val followers = Random.nextInt(1000)

    val jsonString =
      s"""
         |{
         |  "first_name" : "$firstAName",
         |  "last_name" : "$lastName",
         |  "location" : "$location",
         |  "online" : $online,
         |  "followers" : $followers
         |}
         |""".stripMargin

    val jsonMessage = new ProducerRecord[String, String](topic, jsonString)
    producer.send(jsonMessage)
  }

  producer.close()
}
