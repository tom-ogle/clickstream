package com.tomogle.clickstream.stream

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.tomogle.clickstream.stream.sessionization.SessionizeClicksImpl
import org.scalatest.FunSuite

class SessionizeClicksProductionTest extends FunSuite with StreamingSuiteBase {

  val SessionizeClicks = new SessionizeClicksImpl {}.sessionizeClickStream _

  test("Single click") {
    val input = Seq(Seq(KeyedClick(Key("1"), Click("192.168.1.1", 1L))))
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 1L)))
    testOperation[KeyedClick, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks, different IP, different batch") {
    val input = Seq(
      Seq(KeyedClick(Key("1"), Click("192.168.1.1", 1L))),
      Seq(KeyedClick(Key("1"), Click("192.168.1.2", 2L)))
    )
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 1L)),
      Seq(Session("192.168.1.2", startTime = 2L, endTime = 2L)))
    testOperation[KeyedClick, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks in event time order, same IP, same batch") {
    val input = Seq(
      Seq(
        KeyedClick(Key("1"), Click("192.168.1.1", 1L)), KeyedClick(Key("1"), Click("192.168.1.1", 2L))
      )
    )
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 2L)))
    testOperation[KeyedClick, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks out of event time order, same IP, same batch") {
    val input = Seq(
      Seq(KeyedClick(Key("1"), Click("192.168.1.1", 2L)), KeyedClick(Key("1"), Click("192.168.1.1", 1L))
      )
    )
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 2L)))
    testOperation[KeyedClick, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks, same IP, different batch") {
    val input = Seq(
      Seq(KeyedClick(Key("1"), Click("192.168.1.1", 1L)), KeyedClick(Key("1"), Click("192.168.1.1", 10L))
      )
    )
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 1L)),
      Seq(Session("192.168.1.1", startTime = 1L, endTime = 10L)))
    testOperation[KeyedClick, Session](input, SessionizeClicks, expected, ordered = true)
  }

}
