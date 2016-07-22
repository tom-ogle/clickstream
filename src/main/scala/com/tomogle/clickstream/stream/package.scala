package com.tomogle.clickstream

package object stream {
  case class Click(ip: String, timestamp: Long)
  case class Key(keyValue: String)
  case class KeyedClick(key: Key, click: Click)

  case class Session(ip: String, startTime: Long, endTime: Long)
}
