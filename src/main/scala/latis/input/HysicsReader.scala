package latis.input

import java.net.URI

import latis.dataset.Dataset
import latis.dsl._
import latis.model._
import latis.ops._
import latis.util.HysicsUtils._
import latis.util.LatisConfig

/**
 * Read the Hysics granule list dataset, put it into Spark,
 * and apply operation to read and structure the data.
 * Cache the RDD and the LaTiS Dataset so we don't have to reload
 * it into spark each time.
 */
object HysicsReader extends DatasetReader {

  // Redundant but needed ultimately for ReaderOperation (which we don't use with this)
  // (x, y, wavelength) -> radiance
  val model: DataType = ModelParser.parse("(x, y, wavelength) -> radiance").toTry.get

  // Used by the DatasetReader ServiceLoader
  override def read(uri: URI): Dataset = {
    val wlds = {
      val wluri = new URI(uri.toString + "/wavelength.txt")
      HysicsWavelengthReader.read(wluri)
    }
    HysicsGranuleListReader
      .read(uri)                                                 //ix -> uri
      .stride(LatisConfig.getOrElse("hylatis.hysics.stride", 1)) //4200 slit images
      //.toSpark() //TODO: use config?
      .withReader(HysicsImageReader) // ix -> (iy, iw) -> radiance
      .uncurry()                                   // (ix, iy, iw) -> radiance
      .substitute(wlds)
      .substitute(xyCSX)
      // This is the canonical form of the cube, cache here
      .cache2() //TODO: resolve conflict with Dataset.cache
  }
}
