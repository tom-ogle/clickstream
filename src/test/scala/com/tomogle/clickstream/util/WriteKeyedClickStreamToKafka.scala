package com.tomogle.clickstream.util

import java.util.concurrent.TimeUnit

import com.tomogle.clickstream.stream.KeyedClick
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

object WriteKeyedClickStreamToKafka extends KafkaClickWriter with ClickStreams {
  import KafkaClickWriter._

  override val topic = InTopic
  override val config = Config

  def main(args: Array[String]): Unit = {
    sys addShutdownHook {
      // TODO: Add in logging
      println("Closing Kafka Click Writer")
      close()
    }
    loop(Option(100), keyedClickStream())
  }

  def loop(sleepTimeMs: Option[Long], clickStream: Stream[KeyedClick]): Unit = {
    val click = clickStream.head
    println(s"Sending click: $click")
    send(click, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) println(s"Exception: $exception")
        println(s"Callback for offset: ${metadata.offset()}")
      }
    })
    sleepTimeMs foreach TimeUnit.MILLISECONDS.sleep
    loop(sleepTimeMs, clickStream.tail)
  }
}
