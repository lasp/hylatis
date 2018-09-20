package latis.data

import scala.collection.Searching._
import scala.util.Try
import scala.util.Success

/**
 * Manage one-dimensional Function Data as columnar arrays.
 * For evaluation, this uses a binary search on the domain array
 * to get the index into the range array.
 */
case class IndexedFunction1D(as: Array[Any], vs: Array[Any]) extends EvaluatableFunction {
  //TODO: consider computable index (regular spacing)
  //TODO: consider coordinate system function composition
  //TODO: combine with ArrayFunction?
  
  def apply(dd: DomainData): Try[RangeData] = dd match {
    case DomainData(d) =>
      as.search(d)(ScalarOrdering) match {
        case Found(i) => Success(RangeData(vs(i)))
        case InsertionPoint(i) => ??? //TODO: interpolate
      }
  }
  
  def samples: Iterator[Sample] =
    (as zip vs).map(p => (DomainData(p._1), RangeData(p._2))).iterator
}