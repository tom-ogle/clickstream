package com.tomogle.clickstream.stream

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SessionizerApp {

  def main(args: Array[String]) {
    val inputDirectory = {
      val defaultInputDirectory = "src/main/resources/clicks"
      val inputDirectoryParameter = args(0)
      if (inputDirectoryParameter == null) defaultInputDirectory else inputDirectoryParameter
    }
    val batchDurationMs = {
      val defaultBatchDuration = 200L
      val batchDurationParameter = args(1)
      val duration = if (batchDurationParameter == null) defaultBatchDuration else batchDurationParameter.toLong
      Duration(duration)
    }

    val sparkConf = new SparkConf().setAppName("Clickstream sessionizer").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sparkStreamingContext = new StreamingContext(sparkContext, batchDurationMs)
    // translate to clicks
    // transform
    // sessionize
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()

  }

}

object ClickSessionizerPipeline extends (SessionizerContext => Unit) {
  override def apply(context: SessionizerContext): Unit = {
    import context._
    readClicks andThen SessionizeClicks andThen outputSessions
  }
}

case class SessionizerContext(readClicks: ReadClicks,
                              outputSessions: OutputSessions)


trait ReadClicks extends (StreamingContext => DStream[Click])

object SessionizeClicks extends (DStream[Click] => DStream[Session]) {
  override def apply(clicks: DStream[Click]) = {

    val keyedByIp: DStream[(String, (Long, Long))] = clicks.map(click => (click.ip, (click.timestamp, click.timestamp)))
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
trait OutputSessions extends (DStream[Session] => Unit)
