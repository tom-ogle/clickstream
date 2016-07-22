package com.tomogle.clickstream.util

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

class AvroReadWriteFileUtil[T <: SpecificRecord] {

  lazy val datumWriter = new SpecificDatumWriter[T]()
  lazy val datumReader = new SpecificDatumReader[T]()

  def writeToFile(filePath: String, objectsToWriteToFile: Seq[T], schema: Schema): Unit = {
    if (objectsToWriteToFile.nonEmpty) {
      val file = new File(filePath)
      val datumFileWriter = new DataFileWriter[T](datumWriter)
      using(datumFileWriter) { writer =>
        writer.create(schema, file)
        objectsToWriteToFile foreach writer.append
      }
    }
  }

  def readFromFile(filePath: String): List[T] = {
    val file: File = new File(filePath)
    val dataFileReader = new DataFileReader[T](file, datumReader)
    var results = List[T]()
    using(dataFileReader) { reader =>
      while(reader.hasNext) {
        val thisObj = reader.next()
        results = thisObj :: results
      }
    }
    results
  }
}
