package com.tomogle.clickstream.stream.input

import com.tomogle.clickstream.stream.{Click, Key, KeyedClick}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

object KafkaDirectStreamReadClicks {
  val kafkaParams = Map[String, String](
    "group.id" -> "sessionizer-consumer-group",
    "zookeeper.connect" -> "zookeeper:2181",
    "bootstrap.servers" -> "kafka:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "com.tomogle.clickstream.util.BasicClickSerializer",
    "acks" -> "1"
  )
  val topics = Set("InTopic")
}
trait KafkaDirectStreamReadClicks extends ReadClicks {


  override def readClickStream(ssc: StreamingContext): DStream[KeyedClick] = {
    import KafkaDirectStreamReadClicks._
    val inputStream: InputDStream[(Key, Click)] = KafkaUtils.createDirectStream[Key, Click, ClickKeyDecoder, BasicClickDecoder](
      ssc, kafkaParams, topics)

    import ReadClicks._

    convertToKeyedClicks(inputStream)
  }
}

trait CheckpointingKafkaDirectStreamReadClicks extends ReadClicks {

  val checkpointInterval: Duration

  override def readClickStream(ssc: StreamingContext): DStream[KeyedClick] = {
    import KafkaDirectStreamReadClicks._
    val inputStream: InputDStream[(Key, Click)] = KafkaUtils.createDirectStream[Key, Click, ClickKeyDecoder, BasicClickDecoder](
      ssc, kafkaParams, topics)
    inputStream.checkpoint(checkpointInterval)

    import ReadClicks._

    convertToKeyedClicks(inputStream)
  }
}
