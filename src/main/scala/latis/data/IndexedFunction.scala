package latis.data

import scala.collection.Searching._

import latis.util.LatisOrdering

/**
 * Provide a base trait for Cartesian IndexedFunctions of various dimensions.
 */
trait IndexedFunction extends MemoizedFunction {

  implicit val ord: Ordering[Datum] = LatisOrdering.dataOrdering

  def searchDomain(values: Seq[Datum], data: Datum): SearchResult = {
    values.search(data)
  }
    //
    //(values.head, data) match {
    //case (_: Number, data: Number) =>
    //  values.asInstanceOf[Seq[Number]].search(data)
    //case (_: Text, data: Text) =>
    //  values.asInstanceOf[Seq[Text]].search(data)
    //case _ => ??? //TODO error, invalid types

}
