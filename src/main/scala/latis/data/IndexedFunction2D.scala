package latis.data

import scala.collection.Searching._
import scala.util.Try
import scala.util.Success

/**
 * Manage two-dimensional Function Data as columnar arrays.
 * For evaluation, this uses a binary search on the domain arrays
 * to get the indices into the range array.
 * This assumes a Cartesian product domain set.
 */
case class IndexedFunction2D(as: Array[Any], bs: Array[Any], vs: Array[Array[Any]]) extends EvaluatableFunction {
  //TODO: consider smart constructor: IndexedFunction(ds: Array[Data]*)(vs: Array[Data])
  //TODO: assert that sizes align
  //TODO: should range values be RangeData instead of Any so we can have multiple variables?
  
  def apply(dd: DomainData): Try[RangeData] = dd match {
    case DomainData(a, b) =>
      val ia = as.search(a)(ScalarOrdering) match {
        case Found(i) => i
        case InsertionPoint(i) => ??? //TODO: interpolate
      }
      val ib = bs.search(b)(ScalarOrdering) match {
        case Found(i) => i
        case InsertionPoint(i) => ??? //TODO: interpolate
      }
      Success(RangeData(vs(ia)(ib)))
    case _ => ??? //TODO: error
  }
  
  def samples: Iterator[Sample] = {
    val ss = for {
      ia <- 0 until as.length
      ib <- 0 until bs.length
    } yield (DomainData(as(ia), bs(ib)), RangeData(vs(ia)(ib)))
    
    ss.iterator
  }
}