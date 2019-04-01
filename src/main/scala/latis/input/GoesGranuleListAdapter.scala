package latis.input

import java.net.URI

import latis.data._
import latis.util.LatisProperties
import fs2._
import cats.effect.IO

class GoesGranuleListAdapter extends Adapter {
  def apply(uri: URI): SampledFunction = {
    val base = uri.toString //"s3:/goes-001"
      //val imageCount = 3 //LatisProperties.getOrElse("imageCount", "3").toInt
      
//      //TODO: use 0-based index
//      val samples: Stream[IO, Sample] = Stream.range(1, imageCount + 1) map { i =>
//        val uri = f"${base}/goes$i%04d.nc"
//        Sample(DomainData(i), RangeData(uri))
//      }

    val samples = Seq(
      Sample(DomainData(0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C06_G16_s20182301700501_e20182301711273_c20182301711299.nc")),
      Sample(DomainData(1), RangeData(s"$base/OR_ABI-L1b-RadF-M3C07_G16_s20182301700501_e20182301711279_c20182301711312.nc")),
      Sample(DomainData(2), RangeData(s"$base/OR_ABI-L1b-RadF-M3C08_G16_s20182301700501_e20182301711267_c20182301711312.nc"))
    )
      
    SampledFunction.fromSeq(samples)
  }
}