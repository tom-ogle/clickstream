package com.tomogle.clickstream.stream

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.FunSuite

class SessionizeClicksProductionTest extends FunSuite with StreamingSuiteBase {

  test("Single click") {
    val input = Seq(Seq(Click("192.168.1.1", 1L)))
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 1L)))
    testOperation[Click, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks, different IP, different batch") {
    val input = Seq(Seq(Click("192.168.1.1", 1L)), Seq(Click("192.168.1.2", 2L)))
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 1L)),
      Seq(Session("192.168.1.2", startTime = 2L, endTime = 2L)))
    testOperation[Click, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks in event time order, same IP, same batch") {
    val input = Seq(Seq(Click("192.168.1.1", 1L), Click("192.168.1.1", 2L)))
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 2L)))
    testOperation[Click, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks out of event time order, same IP, same batch") {
    val input = Seq(Seq(Click("192.168.1.1", 2L), Click("192.168.1.1", 1L)))
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 2L)))
    testOperation[Click, Session](input, SessionizeClicks, expected, ordered = true)
  }

  test("Two clicks, same IP, different batch") {
    val input = Seq(Seq(Click("192.168.1.1", 1L), Click("192.168.1.1", 10L)))
    val expected = Seq(Seq(Session("192.168.1.1", startTime = 1L, endTime = 1L)),
      Seq(Session("192.168.1.1", startTime = 1L, endTime = 10L)))
    testOperation[Click, Session](input, SessionizeClicks, expected, ordered = true)
  }

}
