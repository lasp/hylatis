package latis.ops

import latis.data._
import latis.input.ModisGeolocationReader
import latis.metadata._
import latis.model._

/**
 * band -> (ix, iy) -> radiance  =>  band -> (longitude, latitude) -> radiance
 */
case class ModisGeoSub() extends UnaryOperation {
  //TODO: impl with RDD mapValues
  
//  // (ix, iy) -> (longitude, latitude)
//  lazy val geoLocation = ModisGeolocationReader().getDataset.data
//  
//  // (ix, iy) -> A  =>  (longitude, latitude) -> A
//  val f: Sample => Sample = (sample: Sample) => sample match {
//    case Sample(domain, range) =>
//      val newDomain = geoLocation(domain) match {
//        case Some(RangeData(lon: OrderedData, lat: OrderedData)) =>
//          DomainData(lon, lat)
//      }
//      Sample(newDomain, range)
//  }
    
  val g: RangeData => RangeData = (range: RangeData) => range match {
    case RangeData(sf: SampledFunction) =>  //NetcdfFunction2
      // (ix, iy) -> (longitude, latitude)
      val geoLocation = ModisGeolocationReader().getDataset.data
      val samples = sf.unsafeForce.samples map {
        case Sample(domain, range) =>
          val newDomain = geoLocation(domain) match {
            case Some(RangeData(lon: OrderedData, lat: OrderedData)) =>
              DomainData(lon, lat)
          }
          Sample(newDomain, range)
      }
      RangeData(SeqFunction(samples))
  }
    
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = data match {
    case rddF: RddFunction => RddFunction(rddF.rdd.mapValues(g))
  }
  
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