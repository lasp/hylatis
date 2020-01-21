package latis.input

import java.net.URI

import cats.effect.IO
import fs2._

import latis.data._
import latis.ops.Operation
import latis.util.LatisConfig

class HysicsGranuleListAdapter extends Adapter {
  def getData(uri: URI, ops: Seq[Operation]): SampledFunction = {
      val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
      val imageCount = LatisConfig.getOrElse("hylatis.hysics.image-count", 4200)
      // Use image count to compute a stride.
      //TODO: use more suitable operations instead of this property
      val stride: Int = 4200 / imageCount
    
      val samples: Stream[IO, Sample] = Stream.range(1, 4201, stride) map { i =>
        val uri = f"${base}/img$i%04d.txt"
        Sample(DomainData(i), RangeData(uri))
      }

      StreamFunction(samples)
    }
}
