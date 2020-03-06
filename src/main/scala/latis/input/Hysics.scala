package latis.input

import java.net.URI

import latis.dataset.Dataset
import latis.ops.Stride
import latis.ops.Uncurry
import latis.util.LatisConfig

class Hysics extends StaticDatasetResolver {
  //TODO: change to HysicsReader with read(uri)

  def datasetIdentifier: String = "hysics"

  val baseUri = new URI("file:///data/s3/hylatis-hysics-001/des_veg_cloud")
  //val baseUri = new URI("file:///not/found")
  //val baseUri = new URI("s3://hylatis-hysics-001/des_veg_cloud")
  //val baseUri: URI = LatisConfig.get("hylatis.hysics.base-uri") match {
  //  case Some(uri) => new URI(uri)
  //  case None => throw LatisException("hylatis.hysics.base-uri not defined")
  //}

  val stride: Seq[Int] = Seq(4200 / LatisConfig.getOrElse("hylatis.hysics.image-count", 4200))

  def getDataset(): Dataset =
    HysicsGranuleListReader
      .read(baseUri) //ix -> uri
      .withOperation(Stride(stride))
      //.restructureWith(RddFunction)
      .withReader(HysicsImageReader) // ix -> (iy, iw) -> radiance
      .withOperation(Uncurry())      // (ix, iy, iw) -> radiance

  private def getWavelengthDataset(): Dataset = ???
}

object Hysics {
  def dataset: Dataset = (new Hysics()).getDataset()
}
