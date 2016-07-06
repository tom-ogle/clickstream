package com.tomogle.clickstream

package object stream {
  case class Click(ip: String, timestamp: Long)

  case class Session(ip: String, startTime: Long, endTime: Long)
}
