package com.tomogle.clickstream.util

import com.tomogle.clickstream.stream.{Click, Key, KeyedClick}

import scala.util.Random

trait ClickStreams {
  def clickStream(): Stream[Click] = Stream.cons[Click](generateClick(), clickStream())
  def keyedClickStream(): Stream[KeyedClick] = Stream.cons[KeyedClick](generateKeyedClick(), keyedClickStream())

  private lazy val ips = Vector("192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4", "192.168.1.5")

  def generateClick(): Click = {
    val ip = ips(Random.nextInt(ips.length))
    val timestamp = Math.abs(Random.nextLong())
    Click(ip = ip, timestamp = timestamp)
  }

  def generateKeyedClick(): KeyedClick = {
    val click = generateClick()
    val key = Key(click.ip)
    KeyedClick(key = key, click = click)
  }

}
