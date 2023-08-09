package com.knoldus
package Subscriber

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.IterableHasAsScala

object dataConsumer extends App {
  val props = new Properties()

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)

  val topic = "quickstart-events"
  consumer.subscribe(Collections.singletonList(topic))

  def processRecords(records: Iterable[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]): Unit = {
    records.foreach { record =>
      println(record)
    }
  }

  while (true) {
    val records = consumer.poll(java.time.Duration.ofMillis(100))
    processRecords(records.asScala)
  }

  consumer.close()
}
