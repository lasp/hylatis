package latis.input

import latis.data._
import latis.metadata._
import latis.model._
import latis.ops._
import java.net.URI

import cats.effect.IO
import fs2._

import latis.dataset.Dataset
import latis.util.LatisException

/**
 * This reader provides a sequence of Hysics image file URLs:
 *   i -> uri
 * The URI used to construct this reader is the base URL to the 
 * directory/bucket where the Hysics images files live.
 * The files are named "img0001.txt" through "img4200.txt".
 */
//class HysicsGranuleListReader(uri: URI) extends AdaptedDatasetReader {
object HysicsGranuleListReader extends AdaptedDatasetReader {

  def metadata: Metadata = Metadata(
    "id" -> "hysics_image_files"
  )

  def model: DataType = Function(
    Scalar(Metadata("ix") + ("type" -> "int")),
    Scalar(Metadata("uri") + ("type" -> "string"))
  )
    
  def adapter: Adapter = new Adapter() {
    //TODO: use GranuleListAdapter

    override def canHandleOperation(op: Operation): Boolean = op match {
      case _: Stride => true
      case _         => false
    }

    def getData(uri: URI, ops: Seq[Operation]): SampledFunction = {
      val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
      val stride: Int = ops.collectFirst {
        case Stride(stride) => stride.head
      }.getOrElse(1)

      val samples: Stream[IO, Sample] = Stream.range(0, 4200, stride) map { i =>
        val uri = f"$base/img${i+1}%04d.txt"
        Sample(DomainData(i), RangeData(uri))
      }

      StreamFunction(samples)
    }
  }
  
}
  
//object HysicsGranuleListReader {
//
//  def read(uri: URI): Dataset = {
//    (new HysicsGranuleListReader).read(uri).getOrElse {
//      val msg = s"Failed to read Hysics granule list dataset: $uri"
//      throw LatisException(msg)
//    }
//  }
//}
//
//  def apply() = {
//    val defaultURI = "s3://hylatis-hysics-001/des_veg_cloud"
//    val uri = LatisConfig.getOrElse("hylatis.hysics.base-uri", defaultURI)
//    new HysicsGranuleListReader(URI.create(uri))
//  }
//
//}
