package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

/**
 * Operation on a dataset that specifies wavelength as the first domain variable,
 * and returns a dataset with the wavelength as the 3rd domain variable, after x and y.
 * In Summary wavelength -> (x, y) -> Rad will become (x, y) -> wavelength -> Rad
 */
case class TransposeWavelengthWithPosition() extends MapOperation {
  
  def makeMapFunction(model: DataType): Sample => Sample = {
    case (DomainData(w, x, y), range) => (DomainData(x, y, w), range)
    //case Sample(n, ns) => Sample(n, transpose(ns))
  }

//  def transpose(data: Seq[Data]): Seq[Data] = data match {
//    case Seq(Integer(wavelength), Integer(x), Integer(y), Integer(rad)) => 
//      Seq(Integer(x), Integer(y), Integer(wavelength), Integer(rad))
//  }
  
  // (x, y) -> wavelength -> Rad
  override def applyToModel(model: DataType): DataType = {
    Function(
      Tuple(Scalar("x"), Scalar("y"), Scalar("wavelength")),
      Scalar("Rad")
    )
  }
  
}