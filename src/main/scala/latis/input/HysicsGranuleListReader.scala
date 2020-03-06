package latis.input

import java.net.URI

import cats.effect.IO
import fs2._

import latis.data._
import latis.metadata._
import latis.model._
import latis.ops._

/**
 * This reader provides a sequence of Hysics image file URLs:
 *   ix -> uri
 * The URI used to construct this reader is the base URL to the
 * directory/bucket where the Hysics images files live.
 * The files are named "img0001.txt" through "img4200.txt".
 */
object HysicsGranuleListReader extends AdaptedDatasetReader {

  def metadata: Metadata = Metadata(
    "id" -> "hysics_image_files"
  )

  def model: DataType = Function(
    Scalar(Metadata("ix") + ("type"  -> "int")),
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

      val samples: Stream[IO, Sample] = Stream.range(0, 4200, stride).map { i =>
        val uri = f"$base/img${i + 1}%04d.txt"
        Sample(DomainData(i), RangeData(uri))
      }

      StreamFunction(samples)
    }
  }

}
