package latis.data

import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * A SampledFunction implemented with a 2D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction2D(array: Array[Array[RangeData]]) extends EvaluatableFunction {
  
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

/*
 * TODO: constructor with Any raw data values, e.g. Array[Array[Double]]
 * wrap it as RangeData
 * OR keep raw data and (un)wrap as needed in code above
 */
 object ArrayFunction2D