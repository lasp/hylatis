package latis.input

import latis.metadata._
import latis.model._
import latis.data._
import java.net.URI
import latis.util.SparkUtils
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import latis.ops._

case class SparkAdapter(model: DataType) extends IterativeAdapter[Sample] {
  
  def recordIterator(uri: URI): Iterator[Sample] = getRDD(uri).toLocalIterator
  
  def parseRecord(record: Sample): Option[Sample] = Option(record)
  
  def getRDD(uri: URI): RDD[Sample] = {
    var rdd = SparkUtils.getRDD(uri.getPath).getOrElse(???).asInstanceOf[RDD[Sample]]
    //assume (row, column, wavelength) -> irradiance
    //apply ops
    for (op <- ops) op match {
      
      case f: Filter =>
        val p = f.makePredicate(model)
        rdd = rdd.filter(p)
      
      case mo: MapOperation =>
        rdd = rdd.map(mo.makeMapFunction(model))
        
      case uc: Uncurry =>
        rdd = rdd.flatMap(uc.makeMapFunction(model))
        
      case RGBImagePivot(vid, r, g, b) =>
        val colors = Set(r,g,b)
        val colorFilter: Sample => Boolean = (sample: Sample) => sample match {
          case (DomainData(_, _, w: Double), _) =>
            if (colors.contains(w)) true else false
        }
        
        val groupByFunction: Sample => DomainData = (sample: Sample) => sample match {
          //assume first 2 vars in domain are row, col
          case Sample(ds, _) => DomainData.fromSeq(ds.take(2))  
        }
        
        val agg: (DomainData, Iterable[Sample]) => Sample = (data: DomainData, samples: Iterable[Sample]) => {
          //samples should be spectral samples at the same pixel
          //after groupBy: (row, column); (row, column, w) -> f
          val ss = samples map {
            case Sample(ds, rs) => Sample(ds.drop(2), rs)  // w -> f
          }
          (data, RangeData(StreamingFunction(ss.iterator))) // (row, column) -> w -> f
        }
          
        val pivot: Sample => Sample = (sample: Sample) => sample match {
          // (row, column) -> w -> f  =>  (row, column) -> (r, g, b)
          // assumes 3 wavelength values have been previously selected
          case (domain, RangeData(SampledFunction(it))) =>
            // Create (r,g,b) tuple from spectrum
            // pivot  w -> f  =>  (r, g, b)
            val colors: Seq[Any] = it.toVector map {
              case (_, RangeData(d)) => d
            }
            
            (domain, RangeData(colors: _*))  // (row, column) -> (r, g, b)
        }
        
        implicit val ord = DomainOrdering
        rdd = rdd.filter(colorFilter)
                 .groupBy(groupByFunction)  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
                 .map(p => agg(p._1, p._2))
                 .map(pivot) //TODO: can we use spark pivot?
                 .sortBy(_._1) //(DomainOrdering) 
    }
    rdd
  }
  
  /**
   * Keep collection of Operations to apply.
   */
  private val ops = ArrayBuffer[Operation]()
  
  /**
   * Handle all Operations, for now.
   */
  override def handleOperation(op: Operation): Boolean = {
    ops += op
    true
  }
}