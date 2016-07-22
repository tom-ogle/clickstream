package com.tomogle.clickstream.stream.sessionization

import com.tomogle.clickstream.stream.{KeyedClick, Session}
import org.apache.spark.streaming.dstream.DStream

trait SessionizeClicks {
  def sessionizeClickStream(clicks: DStream[KeyedClick]): DStream[Session]
}
trait SessionizeClicksImpl extends SessionizeClicks {
  def sessionizeClickStream(clicks: DStream[KeyedClick]): DStream[Session] = {

    val keyedByIp: DStream[(String, (Long, Long))] = clicks.map {
      case KeyedClick(key, click) =>
        (click.ip, (click.timestamp, click.timestamp))
    }
    // timestamp is duplicated to allow the types to work for a single reduceByKey operation
    val minMaxByIp: DStream[(String, (Long, Long))] = keyedByIp.reduceByKey {
      case ((aTimestamp, _), (bTimestamp, _)) =>
        val min = Math.min(aTimestamp, bTimestamp)
        val max = Math.max(bTimestamp, aTimestamp)
        (min, max)
    }
    minMaxByIp.map {
      case (ip, (minTimestamp, maxTimestamp)) =>
        Session(ip, minTimestamp, maxTimestamp)
    }
  }
}
