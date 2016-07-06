package com.tomogle.clickstream.util

import java.io.File

import example.clickstream.AvroClick
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class AvroReadWriteFileUtilTest extends FunSuite with Matchers with BeforeAndAfter {

  val clicksFileName = "target/clicks.avro"

  after {
    val clickFile = new File(clicksFileName)
    if (clickFile.exists()) {
      clickFile.delete()
    }
  }

  test("Basic serialization and deserialization should be possible") {


    val ip = "192.168.1.20"
    val userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36"
    val timestamp1 = 12345678L
    val timestamp2 = 87654321L
    val url = "http://www.example.com/page1.html"
    val referer = "http://www.example.com/index.html"
    val click1 = new AvroClick(ip, timestamp1, url, referer, userAgent)
    val click2 = new AvroClick(ip, timestamp2, url, referer, userAgent)

    val input = List(click1, click2)

    object ClickReadWriteFileUtil extends AvroReadWriteFileUtil[AvroClick]
    // Serialize
    ClickReadWriteFileUtil.writeToFile(clicksFileName, input, AvroClick.getClassSchema)
    // Deserialize
    val results: List[AvroClick] = ClickReadWriteFileUtil.readFromFile(clicksFileName)
    // Check results
    results should have length 2

  }
}
