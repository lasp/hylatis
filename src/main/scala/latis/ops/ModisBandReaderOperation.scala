package latis.ops

import latis.data._
import latis.metadata._
import latis.model._
import java.net.URI
import latis.input.ModisNetcdfAdapter2
import latis.input.ModisGeolocationReader

case class ModisBandReaderOperation() extends MapOperation {
  
  def makeMapFunction(model: DataType): Sample => Sample =
    (sample: Sample) => sample match {
      case Sample(domain, RangeData(Text(s))) =>
        val ss = s.split(",")
        val uri = new URI(ss(0))
        val vname = ss(1)
        val bandIndex = ss(2).toInt
        val adapter = ModisNetcdfAdapter2(vname, bandIndex)
        val data = adapter(uri)
        val range = RangeData(data)
        Sample(domain, range)
    }
    
  override def applyToModel(model: DataType): DataType = model match {
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