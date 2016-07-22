package com.tomogle.clickstream.stream

import com.tomogle.clickstream.stream.input.{KafkaDirectStreamReadClicks, ReadClicks}
import com.tomogle.clickstream.stream.output.{OutputSessions, PrintlnOutputSessions}
import com.tomogle.clickstream.stream.sessionization.{SessionizeClicks, SessionizeClicksImpl}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SessionizerApp {

  def createNewStreamingContextWithPipelineInitialised(checkpointDirectory: String): () => StreamingContext = () => {
    val batchDurationMs = Duration(200L)
    val sparkConf = new SparkConf()
      .setAppName("Clickstream sessionizer")
      .setMaster("local[*]")
      //        .set("spark.executor.cores", "2")
      //        .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.cassandra.connection.host", "cassandra-1")
    //        .set("spark.cassandra.auth.username", "cassandra")
    //        .set("spark.cassandra.auth.password", "cassandra")
    val sparkContext = new SparkContext(sparkConf)
    val sparkStreamingContext = new StreamingContext(sparkContext, batchDurationMs)
    sparkStreamingContext.checkpoint(checkpointDirectory)
    val sessionizerPipeline = SessionizerPipeline()
    sessionizerPipeline.setupStream(sparkStreamingContext)
    sparkStreamingContext
  }

  def main(args: Array[String]) {
    val checkpointDirectory = "/tmp/"
    val sparkStreamingContext = StreamingContext.getOrCreate(checkpointDirectory, createNewStreamingContextWithPipelineInitialised(checkpointDirectory))
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }

}

trait SessionizerPipeline {

  self: ReadClicks with SessionizeClicks with OutputSessions =>

  def setupStream(ssc: StreamingContext) = {
    val keyedClickStream = readClickStream(ssc)
    val sessionStream = sessionizeClickStream(keyedClickStream)
    outputSessionStream(sessionStream)
  }
}

object SessionizerPipeline {
  def apply(): SessionizerPipeline = new SessionizerPipeline()
    with KafkaDirectStreamReadClicks
    with SessionizeClicksImpl
    with PrintlnOutputSessions
}


