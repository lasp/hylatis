package latis.ops

import latis.data._
import latis.metadata._
import latis.model._
import java.net.URI
import latis.input.ModisNetcdfAdapter2
import latis.input.ModisGeolocationReader

case class ModisBandReaderOperation() { //extends MapRangeOperation {
  
  def mapFunction(model: DataType): RangeData => RangeData =
    (range: RangeData) => range match {
      case RangeData(Text(s)) =>
        val ss = s.split(",")
        val uri = new URI(ss(0))
        val vname = ss(1)
        val bandIndex = ss(2).toInt
        val adapter = ModisNetcdfAdapter2(vname, bandIndex)
        val data = adapter.getData(uri)
        RangeData(data)
    }
    
  //override
  def applyToModel(model: DataType): DataType = model match {
    case Function(domain, _) =>
      val range = Function(
        Tuple(
          Scalar(Metadata("id" -> "ix", "type" -> "int")), 
          Scalar(Metadata("id" -> "iy", "type" -> "int"))
        ),
        Scalar(Metadata("id" -> "radiance", "type" -> "float"))
      )
      Function(domain, range)
  }
}
