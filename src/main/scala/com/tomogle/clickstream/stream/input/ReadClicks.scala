package com.tomogle.clickstream.stream.input

import java.io.{ByteArrayInputStream, ObjectInputStream}

import com.tomogle.clickstream.stream.{Click, Key, KeyedClick}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.util.Try

trait ReadClicks {
  def readClickStream(ssc: StreamingContext): DStream[KeyedClick]
}

object ReadClicks {
  def convertToKeyedClicks(keyAndClickStream: DStream[(Key, Click)]): DStream[KeyedClick] = keyAndClickStream.map {
    case (key, click) => KeyedClick(key, click)
  }
}

class ClickKeyDecoder(props: VerifiableProperties) extends Decoder[Key] {
  val encoding =
    if (props == null) "UTF8"
    else props.getString("serializer.encoding", "UTF8")

  override def fromBytes(bytes: Array[Byte]): Key = {
    val keyString = new String(bytes, encoding)
    Key(keyValue = keyString)
  }
}

class BasicClickDecoder(props: VerifiableProperties) extends Decoder[Click] {
  override def fromBytes(bytes: Array[Byte]): Click = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    try {
      val obj = ois.readObject()
      obj.asInstanceOf[Click]
    } finally {
      if (ois != null) Try(ois.close()).failed.foreach(t => println(s"Failed to close stream: ${t.getMessage}"))
      if (bis != null) Try(bis.close()).failed.foreach(t => println(s"Failed to close stream: ${t.getMessage}"))
    }
  }
}
