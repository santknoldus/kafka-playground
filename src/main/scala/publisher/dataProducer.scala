package com.knoldus
package publisher

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties

object dataProducer extends App {
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val topic = "quickstart-events"

  // Sending Normal String as data --------------------------------------------------------------

  val data = Seq("Hello", "My name is Pradyuman", "Testing of Kafka Producer")

  data.foreach( message => producer.send(new ProducerRecord[String, String](topic, message)))

  // --------------------------------------------------------------------------------------------

  producer.close()
}
