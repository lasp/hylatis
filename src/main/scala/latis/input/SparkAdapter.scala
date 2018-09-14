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
          case Sample(_, Array(_, _, ScalarData(w), _)) =>
            if (colors.contains(w.asInstanceOf[Double])) true else false
        }
        
        val groupByFunction: Sample => Data = (sample: Sample) => sample match {
          case Sample(_, Array(row, column, _, _)) => TupleData(row, column)  
        }
        
        val agg: (Data, Iterable[Sample]) => Sample = (data: Data, samples: Iterable[Sample]) => {
          //samples should be spectral samples at the same pixel
          //after groupBy: (row, column); (row, column, w) -> f
          val ss = samples map {
            case Sample(3, ds) => Sample(1, ds.drop(2))  // w -> f
          }
          Sample(2, (data.asInstanceOf[TupleData].elements :+ StreamingFunction(ss.iterator)).toArray)  // (row, column) -> w -> f
        }
          
        val pivot: Sample => Sample = (sample: Sample) => sample match {
          // (row, column) -> w -> f
          case Sample(n, ds) =>
            val domain: Array[Data] = ds.take(n)
            val range: Array[Data] = { // pivot w -> f  to  (r, g, b)
              ds.last.asInstanceOf[SampledFunction].samples map {
                case Sample(_, Array(_, d)) => d
              }
            }.toArray
            
            Sample(n, domain ++ range)  // (row, column) -> (r, g, b)
        }
        
        
        rdd = rdd.filter(colorFilter)
                 .groupBy(groupByFunction)
                 .map(p => agg(p._1, p._2))
                 .map(pivot) //TODO: can we use spark pivot?
                 .sortBy(_.domain) //TODO: define Ordering on Data
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