package example.clickstream.util

import java.io.File

import example.clickstream.Click
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  *
  */
class AvroReadWriteFileUtilTest extends FunSuite with Matchers with BeforeAndAfter {

  val clicksFileName = "clicks.avro"

  after {
    val clickFile = new File(clicksFileName)
    if (clickFile.exists()) {
      clickFile.delete()
    }
  }

  test("Basic serialization and deserialization should be possible") {


    val ip = "192.168.1.20"
    val userAgent = "Chrome"
    val timestamp = "12345678"
    val url = "http://www.example.com/page1.html"
    val referer = "http://www.example.com/index.html"

    val click1 = new Click(ip, userAgent, timestamp, url, referer)
    val click2 = new Click(ip, userAgent, "12345679", url, referer)

    val input = List(click1, click2)

    object ClickReadWriteFileUtil extends AvroReadWriteFileUtil[Click]
    // Serialize
    ClickReadWriteFileUtil.writeToFile(clicksFileName, input, Click.getClassSchema)
    // Deserialize
    val results = ClickReadWriteFileUtil.readFromFile(clicksFileName)
    // Check results
    results should have length 2

  }
}
