package latis.data

import scala.collection.Searching._

import latis.util.LatisException

/**
 * Manage one-dimensional SampledFunction Data as columnar sequences.
 * For evaluation, this uses a binary search on the domain values
 * to get the index into the range values.
 */
case class IndexedFunction1D(xs: Seq[Datum], vs: Seq[TupleData]) extends IndexedFunction {
  //TODO: assert that sizes are consistent
  //Note, using Seq instead of invariant Array to get variance
  //TODO: prevent diff types of OrderedData, e.g. mixing NumberData and TextData, or IntData and DoubleData
  // [T <: OrderedData]

  override def apply(value: TupleData): Either[LatisException, TupleData] = value match {
    case TupleData((d: Datum) :: Nil) =>
      searchDomain(xs, d) match {
        case Found(i) => Right(vs(i))
        case InsertionPoint(i) =>
          //TODO: interp
          if (i >= vs.length) Right(vs(i-1))
          else Right(vs(i))
        case _ =>
          val msg = s"No sample found matching $value"
          Left(LatisException(msg))
      }
  }
  
  /**
   * Provide a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def sampleSeq: Seq[Sample] =
    (xs zip vs) map { case (x, v) => Sample(DomainData(x), RangeData(v.elements)) }

}
