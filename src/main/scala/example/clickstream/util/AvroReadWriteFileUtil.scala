package example.clickstream.util

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

/**
  *
  */
class AvroReadWriteFileUtil[T <: SpecificRecord] {

  lazy val datumWriter = new SpecificDatumWriter[T]()
  lazy val datumReader = new SpecificDatumReader[T]()

  def writeToFile(filePath: String, objectsToWriteToFile: Seq[T], schema: Schema): Unit = {
    if (objectsToWriteToFile.nonEmpty) {
      val file: File = new File(filePath)
      val datumFileWriter = new DataFileWriter[T](datumWriter)
      datumFileWriter.create(schema, file)
      try {
        objectsToWriteToFile foreach datumFileWriter.append
      } finally {
        datumFileWriter.close()
      }
    }
  }

  def readFromFile(filePath: String): List[T] = {
    val file: File = new File(filePath)
    val dataFileReader = new DataFileReader[T](file, datumReader)
    var results = List[T]()
    try {
      while(dataFileReader.hasNext) {
        val thisObj = dataFileReader.next()
        results = thisObj :: results
      }

    } finally {
      dataFileReader.close()
    }
    results
  }
}
