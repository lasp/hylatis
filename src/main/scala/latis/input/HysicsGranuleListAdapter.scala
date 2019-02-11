package latis.input

import java.net.URI

import latis.data._
import latis.util.LatisProperties
import fs2._
import cats.effect.IO

class HysicsGranuleListAdapter extends Adapter {
  def apply(uri: URI): SampledFunction = {
      val base = uri.toString //"s3:/hylatis-hysics-001/des_veg_cloud"
      val imageCount = LatisProperties.getOrElse("imageCount", "4200").toInt
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