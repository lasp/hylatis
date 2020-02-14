package latis.output

import cats.effect.IO
import fs2.Stream
import geotrellis.proj4.LatLng
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace

import latis.dataset.Dataset
import latis.data._
import latis.data.Data._
import latis.util.LatisException

/**
 * Makes an RGB GeoTiff from a Dataset.
 *
 * This encoder makes assumptions about the dataset:
 *
 * - The dataset must be of the form (lat, lon) -> (r, g, b).
 * - The dataset is ordered by longitude primarily, latitude secondarily.
 * - All range values and types are bytes.
 * - The dataset has at least two samples. A LatisException is thrown otherwise.
 * - The dataset forms a regular grid with no holes.
 * - The regular grid is aligned with lat lon, and not skewed.
 *
 * Due to the way pixel size is estimated, images near the poles will not
 * project accurately.
 *
 */
class GeoTiffEncoder extends Encoder[IO, MultibandGeoTiff] {
  import GeoTiffEncoder._

  /** Encodes a dataset to a single-element stream. */
  override def encode(dataset: Dataset): Stream[IO, MultibandGeoTiff] =
    dataset.samples
      .fold(List.empty[Sample])(_ :+ _)
      .map(rgb2geotiff)

  private def rgb2geotiff(datasetList: List[Sample]): MultibandGeoTiff = {
    if (datasetList.length < 2) {
      throw LatisException("dataset must have at least two samples.")
    }
    val (redArr, greenArr, blueArr) = datasetList.foldLeft(
      (Array[Byte](), Array[Byte](), Array[Byte]())
    )(addSampleToRgbArray)
    val (cols, rows) = getDimensions(datasetList)

    MultibandGeoTiff(
      MultibandTile(
        Array(
          ByteArrayTile(redArr, rows, cols).rotate90(),
          ByteArrayTile(greenArr, rows, cols).rotate90(),
          ByteArrayTile(blueArr, rows, cols).rotate90()
        )
      ),
      getExtent(datasetList, cols, rows),
      LatLng,
      GeoTiffOptions(storageMethod = Tiled(cols, rows), colorSpace = ColorSpace.RGB)
    )
  }

  private def addSampleToRgbArray(
    arr: (Array[Byte], Array[Byte], Array[Byte]),
    s: Sample
  ): (Array[Byte], Array[Byte], Array[Byte]) = {
    val (r, g, b) = rgb(s)
    (arr._1 :+ r, arr._2 :+ g, arr._3 :+ b)
  }

  /** returns the dimensions of the image as (columns, rows) */
  private def getDimensions(datasetList: List[Sample]): (Int, Int) = {
    // count the number of rows while latitude is increasing
    val rows = datasetList
      .zip(datasetList.tail)
      .takeWhile { case (a, b) => lat(a) < lat(b) }
      .length + 1
    val numPixels = datasetList.length
    val cols      = numPixels / rows
    (cols, rows)
  }

  /** returns the extent computed from the bottom left and top right Sample */
  private def getExtent(datasetList: List[Sample], cols: Int, rows: Int): Extent = {
    val lat1 = lat(datasetList.head)
    val lon1 = lon(datasetList.head)
    val lat2 = lat(datasetList.last)
    val lon2 = lon(datasetList.last)
    // figure out the size of a pixel in degrees. Degrees lat and degrees lon are
    // not interchangeable like this, but it's a decent first approximation as long
    // as we're not near the poles.
    val dLatPerPixel = if (rows == 1) {
      (lon2 - lon1) / (cols - 1)
    } else (lat2 - lat1) / (rows - 1)
    val dLonPerPixel = if (cols == 1) dLatPerPixel else (lon2 - lon1) / (cols - 1)
    Extent(
      lon1 - (dLonPerPixel / 2),
      lat1 - (dLatPerPixel / 2),
      lon2 + (dLonPerPixel / 2),
      lat2 + (dLatPerPixel / 2)
    )
  }

}

object GeoTiffEncoder {

  /** returns the latitude from a Sample */
  private def lat(s: Sample): Double = s match {
    case Sample(DomainData(Number(lat), Number(_)), RangeData(Number(_), Number(_), Number(_))) =>
      lat
  }

  /** returns the longitude from a Sample */
  private def lon(s: Sample): Double = s match {
    case Sample(DomainData(Number(_), Number(lon)), RangeData(Number(_), Number(_), Number(_))) =>
      lon
  }

  /** returns the red, green, and blue components from a Sample */
  private def rgb(s: Sample): (Byte, Byte, Byte) = s match {
    case Sample(DomainData(_, _), RangeData(r: ByteValue, g: ByteValue, b: ByteValue)) =>
      (r.value, g.value, b.value)
  }

}
