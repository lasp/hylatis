package latis.ops

import latis.data._
import latis.input.ModisGeolocationReader
import latis.metadata._
import latis.model._
import latis.util.SparkUtils

/**
 * band -> (ix, iy) -> radiance  =>  band -> (longitude, latitude) -> radiance
 */
case class ModisGeoSub() extends MapRangeOperation {
  //TODO: make a Dataset that wraps a broadcast variable
  
  /**
   * Read the geo location data and broadcast it
   */
  def broadcastGeoLocation = {
    val data: ArrayFunction2D = ??? //ModisGeolocationReader().getDataset.data.asInstanceOf[ArrayFunction2D]
    SparkUtils.sparkContext.broadcast(data)
  }
    
  /**
   * Define a function to substitute the domain indices
   * with the geo locations.
   * Note that this operates on the range only so we can use
   * mapValues without disturbing the keys and partitions.
   */
  def mapFunction(model: DataType): RangeData => RangeData = {
    // Get the broadcast geo locations
    val bcGeoLoc = broadcastGeoLocation
    
    (range: RangeData) => range match {
    case RangeData(sf: SampledFunction) =>
      // (ix, iy) -> (longitude, latitude)
      // Get the geo locations from the broadcast variable
      val geoLocation: ArrayFunction2D = bcGeoLoc.value
      val samples = sf.unsafeForce.sampleSeq map {
        case Sample(domain, range) =>
          val newDomain = geoLocation(domain) match {
            case Some(RangeData(lon: Datum, lat: Datum)) =>
              DomainData(lon, lat)
          }
          Sample(newDomain, range)
      }
      RangeData(SeqFunction(samples))
    }
  }
    
//  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = data match {
//    case rddF: RddFunction => RddFunction(rddF.rdd.mapValues(g))
//    case _ => throw new RuntimeException("ModisGeoSub only works in Spark")
//    //TODO: support non-spark usage
//  }
//  
  override def applyToModel(model: DataType): DataType = model match {
    case Function(domain, _) =>
      val range = Function(
        Tuple(
          Scalar(Metadata("id" -> "longitude", "type" -> "double")), 
          Scalar(Metadata("id" -> "latitude", "type" -> "double"))
        ),
        Scalar(Metadata("id" -> "radiance", "type" -> "float"))
      )
      Function(domain, range)
  }
  
}
