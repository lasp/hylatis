package latis.input

import latis.model._
import latis.metadata._
import java.net.URI
import latis.ops._

case class HysicsWavelengthsReader(uri: URI) extends AdaptedDatasetSource {
    
  // iw -> wavelength
  val model = Function(
    Tuple(
      Scalar(Metadata("row") + ("type" -> "int")),
      Scalar(Metadata("iw") + ("type" -> "int"))
    ),
    Scalar(Metadata("wavelength") + ("type" -> "double"))
  )
   
  override def metadata = Metadata(
    "id" -> "hysics_wavelengths"
  )
  
  override def operations: Seq[Operation] = Seq(
    //Projection("iw","wavelength") //TODO: need to replace "row" with index
  )
    
  def adapter: Adapter = new MatrixTextAdapter(TextAdapter.Config(), model)
}