package latis.input

import latis.data.RddFunction
import latis.data.Sample
import latis.util.SparkUtils

import java.net.URI

import org.apache.spark.rdd.RDD

/**
 * Adapter that resolves a URI to a Spark RDD and wraps it in a RddFunction.
 */
case class SparkAdapter() extends Adapter {
  
  def apply(uri: URI): RddFunction = {
//    var rdd = SparkUtils.getRDD(uri.getPath).getOrElse(???).asInstanceOf[RDD[Sample]]
//    RddFunction(rdd)
    ???
  }
          
//      case RGBImagePivot(vid, r, g, b) =>
//        val colors = Set(r,g,b)
//        val colorFilter: Sample => Boolean = (sample: Sample) => sample match {
//          case (DomainData(_, _, w: Double), _) =>
//            if (colors.contains(w)) true else false
//        }
//        
//        val groupByFunction: Sample => DomainData = (sample: Sample) => sample match {
//          //assume first 2 vars in domain are row, col
//          case Sample(ds, _) => DomainData.fromSeq(ds.take(2))  
//        }
//        
//        val agg: (DomainData, Iterable[Sample]) => Sample = (data: DomainData, samples: Iterable[Sample]) => {
//          //samples should be spectral samples at the same pixel
//          //after groupBy: (row, column); (row, column, w) -> f
//          val ss = samples.toVector map {
//            case Sample(ds, rs) => Sample(ds.drop(2), rs)  // w -> f
//          }
//          val stream: Stream[IO, Sample] = Stream.eval(IO(ss)).flatMap(Stream.emits(_))
//          (data, RangeData(StreamFunction(stream))) // (row, column) -> w -> f
//        }
//          
//        val pivot: Sample => Sample = (sample: Sample) => sample match {
//          // (row, column) -> w -> f  =>  (row, column) -> (r, g, b)
//          // assumes 3 wavelength values have been previously selected
//          case (domain, RangeData(SampledFunction(ss))) =>
//            // Create (r,g,b) tuple from spectrum
//            // pivot  w -> f  =>  (r, g, b)
//            val colors = ss.compile.toVector.unsafeRunSync() map {
//              case (_, RangeData(d)) => d
//            }
//            
//            (domain, RangeData(colors: _*))  // (row, column) -> (r, g, b)
//        }
//        
//        implicit val ord = DomainOrdering
//        rdd = rdd.filter(colorFilter)
//                 .groupBy(groupByFunction)  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
//                 .map(p => agg(p._1, p._2))
//                 .map(pivot) //TODO: can we use spark pivot?
//                 .sortBy(_._1) //(DomainOrdering) 

}