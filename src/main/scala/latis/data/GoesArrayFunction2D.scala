package latis.data

import scala.language.postfixOps
import scala.math._
import latis.util.GOESUtils.GOESGeoCalculator

/**
 * Hack for GOES demo
 */
case class GoesArrayFunction2D(array: Array[Array[RangeData]]) extends MemoizedFunction {
  
  val calc = GOESGeoCalculator("GOES_EAST")
  
  override def apply(d: DomainData): Option[RangeData] = d match {
    //TODO: support any integral type
    //TODO: handle index out of bounds
    case DomainData(i: Int, j: Int) => Option(array(i)(j))
    case DomainData(lon: Double, lat: Double) => 
      val (y, x) = calc.geoToYX((lat, lon)).get
      val i = round(y).toInt
      val j = round(x).toInt
      Option(array(i)(j))
  }
  
  def samples: Seq[Sample] = 
    Seq.tabulate(array.length, array(0).length) { 
      (i, j) => Sample(DomainData(i, j), array(i)(j)) 
    } flatten

}

object GoesArrayFunction2D extends FunctionFactory {

  def fromSamples(samples: Seq[Sample]): MemoizedFunction = samples match {
    case Seq() => ???              // TODO: figure out how to handle error
    case _ =>
      samples.last.domain match {
        case x +: xs => 
          val y = xs.head
          val xSize: Int = x.toString.toInt + 1
          val ySize: Int = y.toString.toInt + 1
          val array = Array.ofDim[RangeData](xSize, ySize)
          for {
            i <- 0 until xSize
            j <- 0 until ySize
          } array(i)(j) = samples(j + i * ySize).range
          GoesArrayFunction2D(array)
        case _ => ???              // TODO: figure out how to handle error
      }
  }
    
}
  