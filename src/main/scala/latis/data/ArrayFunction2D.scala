package latis.data

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import latis.data.Index

case class ArrayFunction2D(array: Array[Array[Data]]) extends SampledFunction with EvaluatableFunction {
  
  def apply(d: Data): Try[Data] = d match {
    case TupleData(Index(i), Index(j)) => Success(array(i)(j))
    case _ => Failure(new RuntimeException("Failed to evaluate ArrayFunction2D"))
  }
  
  def samples: Iterator[Sample] = {
    val ss = for {
      i <- 0 until array.length
      j <- 0 until array(0).length
    } yield Sample(2, Array(Index(i), Index(j), array(i)(j)))
    
    ss.iterator
  }
}