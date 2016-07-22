package com.tomogle.clickstream.stream.output

import com.datastax.spark.connector.SomeColumns
import com.tomogle.clickstream.stream.Session
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

trait OutputSessions {
  def outputSessionStream(sessionStream: DStream[Session]): Unit
}

trait PrintlnOutputSessions extends OutputSessions {
  override def outputSessionStream(sessionStream: DStream[Session]): Unit = sessionStream.foreachRDD {
    _.foreach(println)
  }
}

trait CassandraOutputSessions extends OutputSessions {
  val keyspaceName: String
  val tableName: String
  val columns = SomeColumns("ip", "startTime", "endTime")
  override def outputSessionStream(sessionStream: DStream[Session]): Unit = sessionStream.foreachRDD { (sessions: RDD[Session]) =>
    import com.datastax.spark.connector._
    sessions.saveToCassandra(keyspaceName, tableName, columns)
  }
}
