package latis.ops

import latis.data._
import latis.model._
import latis.util.GOESUtils._

import scala.math._
import latis.metadata.Metadata

/*
 * assume resampling applies to nested function
 */
case class Resample(domainSet: DomainSet) extends MapRangeOperation {
  //TODO: nothing specifically "geo" about this, or grid
  //TODO: Resample vs Resampling
  //  ResampleGrid vs GridResampling
  /*
   * TODO: keep csx in separate operation that can be composed with this?
   * need types, assume that csx takes us from SF.domain to domainSet type
   * csx as Dataset? 
   * *csx as Operation
   *   would encapsulate Dataset
   *   everything has to happen via an Operation
   *   e.g. make Dataset act like a function
   */
  
  /*
   * Reconcile this with GroupByBin
   * this uses map calling SF.resample
   * GBB uses groupBy then agg
   * could GBB do this via SF.resample?
   *   feed it a Resampling Op?
   * what would determine the technique used?
   *   not the SF impl?
   *   maybe a trait?
   *   based on topology?
   *   modis need binning since there is no fast way to eval
   *     irregular grid, no csx from geo->native
   *  *If domainSet is a BinSet then use GBB?
   *    but all the same in the DSL
   *   
   * include csx
   *   resample(domainSet, csx=identity)
   * if dataset has type A -> B
   *   and domainSet has type:
   *     A: no csx
   *     C: need csx with type:
   *       C -> A: compose, eval
   *       A -> C: in not invertible bin
   * smart constructor that actually returns GBB? would need factory
   * GBB assumes outer function
   *   just build it into the map function
   *       
   * do only for nested function? RangeOperation
   *   no partitioning, mapValues
   * 
   * keep in mind need to modify Dataset not just data
   */

  /**
   *  Make function to resample the nested Function
   *  in the range of each sample.
   */
  def mapFunction(model: DataType): RangeData => RangeData = {
    // Use GroupByBin with NearestNeighbor aggregation
    val gbb = GroupByBin(domainSet) //, NearestNeighborAggregation())
    val innerModel = model match {
      case Function(_, range) => range
    }
    (range: RangeData) => range match {
      case RangeData(sf: SampledFunction) =>
        RangeData(gbb.applyToData(sf, innerModel))
    }
    
//    // Default case: simply delegate to sf.resample
//    (sample: Sample) => sample match {
//      case Sample(domain, RangeData(sf: SampledFunction)) =>
//        Sample(domain, RangeData(sf.resample(domainSet)))
//    }
  }
  
  /**
   *  Replace the domain in nested Function of the model.
   */
  override def applyToModel(model: DataType): DataType = model match {
    case Function(domain, Function(_, range)) => Function(
      domain, Function(domainSet.model, range)
    )
  }
}

