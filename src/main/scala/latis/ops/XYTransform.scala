package latis.ops

import latis.model._
import latis.data._
import latis.util.HysicsUtils
import latis.metadata._

case class XYTransform() extends MapOperation {
  // (ix, iy) -> ?  =>  (x, y) -> ?
  
  override def makeMapFunction(model: DataType):  Sample => Sample =
    (sample: Sample) => sample match {
      case Sample(DomainData(Index(ix), Index(iy)), range) =>
        val (x, y) = HysicsUtils.indexToXY((ix, iy))
        Sample(DomainData(x, y), range)
    }
  
  override def applyToModel(model: DataType): DataType = model match {
    case Function(_, range) => 
      Function(
        Tuple(
          Scalar(Metadata("x") + ("type" -> "double")),
          Scalar(Metadata("y") + ("type" -> "double"))
        ),
        range
      )
  }
}