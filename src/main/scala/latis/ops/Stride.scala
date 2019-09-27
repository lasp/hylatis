package latis.ops

import latis.data.SampledFunction
import latis.model.DataType
import latis.data.NetcdfFunction

case class Stride(stride: Seq[Int]) extends UnaryOperation {
  
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = data match {
    case ncf: NetcdfFunction => ncf.stride(stride)
    case _ => ??? //TODO: impl for any SampledFunction
  }
}