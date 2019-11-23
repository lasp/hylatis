package latis.ops

import latis.data.SampledFunction
import latis.model.DataType
import latis.data.NetcdfFunction

case class Stride(stride: Seq[Int]) extends UnaryOperation {
  
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = data match {
    case ncf: NetcdfFunction => ncf.stride(stride)
    //TODO: support nD stride for Cartesian Datasets
    case sf: SampledFunction =>
      // Assume the stride is 1D
      val samples = sf.streamSamples
        .chunkN(stride(0))
        .map(_(0))
      SampledFunction(samples)
  }
}

object Stride {
  def apply(n: Int): Stride = Stride(Seq(n))
}
