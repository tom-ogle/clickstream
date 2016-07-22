package com.tomogle.clickstream.util

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util
import java.util.concurrent.Future

import com.tomogle.clickstream.stream.{Click, KeyedClick}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer

import scala.util.Try

object KafkaClickWriter {
  // FIXME: Get these params from config file
  val InTopic = "InTopic"
  val Config = Map[String, AnyRef](
    // Many params specific for 0.8.2.1
//    "metadata.fetch.timeout.ms" -> "30000",
//    "timeout.ms" -> "10000",
    "bootstrap.servers" -> "kafka:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "com.tomogle.clickstream.util.BasicClickSerializer",
    "acks" -> "1"
  )
  def apply(): KafkaClickWriter = new KafkaClickWriter {
    override val topic = InTopic
    override val config = Config
  }
}
trait KafkaClickWriter {
  val topic: String
  val config: Map[String, AnyRef]
  private lazy val producer = {
    import collection.JavaConverters._
    val producerConfig = config.asJava
    new KafkaProducer[String, Click](producerConfig)
  }


  def send(keyedClick: KeyedClick): Future[RecordMetadata] = {
    val record = new ProducerRecord[String, Click](topic, keyedClick.key.keyValue, keyedClick.click)
    producer.send(record)
  }

  def send(keyedClick: KeyedClick, callbackOnAck: Callback): Future[RecordMetadata] = {
    val record = new ProducerRecord[String, Click](topic, keyedClick.key.keyValue, keyedClick.click)
    producer.send(record, callbackOnAck)
  }

  def close(): Unit = producer.close()

}
// FIXME: Replace with Avro serialization
class BasicClickSerializer extends Serializer[Click] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: Click): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    try {
      oos.writeObject(data)
      bos.toByteArray
    } finally {
      if (oos != null) Try(oos.close()).failed.foreach(t => println(s"Failed to close stream: ${t.getMessage}"))
      if (bos != null) Try(bos.close()).failed.foreach(t => println(s"Failed to close stream: ${t.getMessage}"))
    }

  }

  override def close(): Unit = ()
}
