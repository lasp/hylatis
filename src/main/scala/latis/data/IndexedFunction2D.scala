package latis.data

import scala.collection.Searching._

import latis.util.LatisException

/**
 * Manage two-dimensional Cartesian SampledFunction Data as columnar arrays.
 * The Cartesian nature allows the domain values for each dimension to be
 * managed separately.
 * For evaluation, this uses a binary search on the domain values
 * to get the indices into the range values.
 */
case class IndexedFunction2D(xs: Seq[Datum], ys: Seq[Datum], vs: Seq[Seq[RangeData]]) extends IndexedFunction {
  //TODO: assert that sizes are consistent
  
  //override def apply(value: DomainData): Either[LatisException, RangeData] = value match {
  //  case DomainData(x, y) =>
  //    (searchDomain(xs, x), searchDomain(ys, y)) match {
  //      case (Found(i), Found(j)) => Right(vs(i)(j))
  //      //TODO: interpolate
  //      case _ =>
  //        val msg = s"No sample found matching $value"
  //        Left(LatisException(msg))
  //    }
  //  case _ =>
  //    val msg = s"Invalid evaluation value for IndexedFunction2D: $value"
  //    Left(LatisException(msg))
  //}
  
  /**
   * Provide a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def sampleSeq: Seq[Sample] = {
    for {
      ia <- xs.indices
      ib <- ys.indices
    } yield Sample(DomainData(xs(ia), ys(ib)), vs(ia)(ib))
  }
}
