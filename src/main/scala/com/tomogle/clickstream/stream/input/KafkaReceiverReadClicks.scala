package com.tomogle.clickstream.stream.input

import com.tomogle.clickstream.stream.{Click, Key, KeyedClick}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaReceiverReadClicks {
  // TODO: Get from config file
  val kafkaParams = Map[String, String](
    "group.id" -> "sessionizer-consumer-group",
    "zookeeper.connect" -> "zookeeper:2181",
    "bootstrap.servers" -> "kafka:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "com.tomogle.clickstream.util.BasicClickSerializer",
    "acks" -> "1"
  )
  val topicName = "InTopic"
  val numberOfPartitions = 3
  val topics = Map[String, Int](
    topicName -> numberOfPartitions
  )
  val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2
}
trait KafkaReceiverReadClicks extends ReadClicks {
  // Write Ahead Log should be enabled to avoid data loss when using this receiver
  import KafkaReceiverReadClicks._

  override def readClickStream(ssc: StreamingContext): DStream[KeyedClick] = {
    val inputStream = KafkaUtils.createStream[Key, Click, ClickKeyDecoder, BasicClickDecoder](
      ssc,
      kafkaParams,
      topics,
      storageLevel)
    import ReadClicks._

    convertToKeyedClicks(inputStream)
  }
}

