package latis.ops

import latis.data._
import latis.metadata._

/**
 * Operation on a dataset that specifies wavelength as the first domain variable,
 * and returns a dataset with the wavelength as the 3rd domain variable, after x and y.
 * In Summary wavelength -> (x, y) -> rad will become (x, y) -> wavelength -> rad
 */
case class TransposeWavelengthWithPosition() extends MapOperation {
  def makeMapFunction(model: DataType): Sample => Sample = (s: Sample) => {
    s match {
      case Sample(n, ns) => Sample(n, transpose(ns))
    }
  }

  def transpose(data: Seq[Data]): Seq[Data] = data match {
    case Seq(Integer(wavelength), Integer(x), Integer(y), Integer(rad)) => 
      Seq(Integer(x), Integer(y), Integer(wavelength), Integer(rad))
  }
  
  // (x, y) -> wavelength -> rad
  override def applyToModel(model: DataType): DataType = {
    FunctionType(
      TupleType(ScalarType("x"), ScalarType("y")),
      FunctionType(
        ScalarType("wavelength"),
        ScalarType("rad")
      )
    )
  }
  
}