package latis.input

import latis.metadata._
import latis.model._
import latis.data._
import java.net.URI
import latis.util.SparkUtils
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import fs2._
import cats.effect.IO


case class SparkAdapter(model: DataType) extends StreamingAdapter[Sample] {
  
  def recordStream(uri: URI): Stream[IO, Sample] = Stream.fromIterator[IO, Sample](getRDD(uri).toLocalIterator)
  
  def parseRecord(record: Sample): Option[Sample] = Option(record)
  
  /*
   * TODO: encapsulate RDD as a SampledFunction
   * impl the FunctionalAlgebra
   * SparkAdapter then only needs to return the RDD wrapped as an RddFunction
   * then we don't need to handle ops here
   *   but ops on SampledFunction vs Dataset
   * ops on dataset
   *   delegate to SampledFunction
   *   do what the subclass can do then get the Stream and apply the rest
   *   who orchestrates that? DatasetSource? getDataset(ops)
   * ops on existing dataset
   *   also impl algebra by delegating to SampledFunction?
   * or should all this abstraction be at the Dataset level instead of SampledFunction?
   *   SF has no metadata
   *   Dataset[F](md, model, data:F) ?
   * who makes the functions for map, filter...?
   *   Operation? also has access to model, get position of "a" for selection
   *   
   */
  
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
        
//      case uc: Uncurry =>
//        rdd = rdd.flatMap(uc.makeMapFunction(model))
        
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
          val ss = samples.toVector map {
            case Sample(ds, rs) => Sample(ds.drop(2), rs)  // w -> f
          }
          val stream: Stream[IO, Sample] = Stream.eval(IO(ss)).flatMap(Stream.emits(_))
          (data, RangeData(StreamFunction(stream))) // (row, column) -> w -> f
        }
          
        val pivot: Sample => Sample = (sample: Sample) => sample match {
          // (row, column) -> w -> f  =>  (row, column) -> (r, g, b)
          // assumes 3 wavelength values have been previously selected
          case (domain, RangeData(SampledFunction(ss))) =>
            // Create (r,g,b) tuple from spectrum
            // pivot  w -> f  =>  (r, g, b)
            val colors = ss.compile.toVector.unsafeRunSync() map {
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
//TODO: handle ops
//  override def handleOperation(op: Operation): Boolean = {
//    ops += op
//    true
//  }
}