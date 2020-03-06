package latis.ops

import java.net.URI

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import latis.data._
import latis.input.GoesImageReader
import latis.model._
import latis.util._

/**
 * Operation on a granule list dataset to get data for each URI.
 */
case class GoesImageReaderOperation() extends MapRangeOperation {
  //TODO: Use ReaderOperation, but it needs reader to be serialized for spark

  def mapFunction(model:  DataType): Data => Data = {
    //TODO: avoid reader in the closure, needs to be serialized for spark
    (data: Data) => data match {
      case Text(u) =>
        val uri = Try(new URI(u)) match {
          case Success(uri) => uri
          case Failure(e) =>
            val msg = s"Invalid URI: $u"
            throw LatisException(msg, e)
        }
        GoesImageReader().read(uri).unsafeForce().data
      //TODO: deal will errors from unsafeForce
      case _ =>
        val msg = "URI variable must be of type text"
        throw LatisException(msg)
    }
  }

  def applyToModel(model: DataType): DataType = model match {
    case Function(d, _) => Function(d, GoesImageReader().model)
  }

}
