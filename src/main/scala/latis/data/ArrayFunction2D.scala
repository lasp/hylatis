package latis.data

import scala.util.Try
import scala.util.Success
import scala.util.Failure

case class ArrayFunction2D(array: Array[Array[RangeData]]) extends SampledFunction with EvaluatableFunction {
  
  def apply(d: DomainData): Try[RangeData] = d match {
    case DomainData(i: Int, j: Int) => Success(array(i)(j))
    case _ => Failure(new RuntimeException("Failed to evaluate ArrayFunction2D"))
  }
  
  def samples: Iterator[Sample] = {
    val ss = for {
      i <- 0 until array.length
      j <- 0 until array(0).length
    } yield (DomainData(i, j), array(i)(j))
    
    ss.iterator
  }
}