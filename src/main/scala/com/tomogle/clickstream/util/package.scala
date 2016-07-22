package com.tomogle.clickstream

import scala.util.Try

package object util {

  def using[T <: {def close()} ](resource: T)(code: T => Unit) = {
    try {
      code(resource)
    } finally if (resource != null) resource.close()
  }

  def using[T <: {def close()} ](resources: Seq[T])(code: Seq[T] => Unit) = {
    try {
      code(resources)
    } finally {
      resources foreach { resource =>
        val tryToClose = Try(resource.close())
        //FIXME: Handle better
        tryToClose.failed.foreach { t =>
          println(s"Failed to close resource: " + t.getMessage)
        }
      }
    }
  }

}
