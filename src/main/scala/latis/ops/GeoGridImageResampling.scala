package latis.ops

import latis.data._
import latis.model._
import latis.util.GOESUtils._

import scala.math._
import latis.metadata.Metadata


case class GeoGridImageResampling(
  lon1: Double, lat1: Double, 
  lon2: Double, lat2: Double, 
  pixels: Int
) extends MapOperation {
  //TODO: assert lon1 < lon2, lat1 < lat2, and within bounds [-180,180), [-90, -90]
  
  /**
   * Compute x and y dimension size from total requested pixel count
   * preserving the aspect ratio (assuming lat degrees = lon degrees).
   */
  val nx: Int = Math.round(Math.sqrt((lon2-lon1) * pixels / (lat2-lat1))).toInt
  val ny: Int = Math.round(pixels.toFloat / nx)
  
  /**
   * Define the target domain set with image (row-column) ordering.
   */
  //TODO: let image writer deal with ordering? we depend on numeric ordering
  lazy val grid: DomainSet = {
    val dx = (lon2 - lon1) / nx
    val dy = (lat2 - lat1) / ny
    val dds = for {
      lat <- Range(ny-1, 0, -1).map(lat1 + dy * _) // reverse lat for image order
      lon <- (0 until nx).map(lon1 + dx * _)
    } yield DomainData(lat, lon)
    DomainSet(dds)
  }

  /**
   *  Make function to evaluate the nested Function in the range of each sample.
   */
  def mapFunction(model: DataType): Sample => Sample = 
    (sample: Sample) => sample match {
      case Sample(domain, RangeData(sf: SampledFunction)) =>
        ??? //Sample(domain, RangeData(sf.resample(grid)))
    }
  
  
  /**
   *  Replace the domain in nested Function of the model.
   */
  override def applyToModel(model: DataType): DataType = model match {
    case Function(domain, Function(_, range)) => Function(
      domain, Function(Tuple(Scalar("latitude"), Scalar("longitude")), range)
    )
  }
}
