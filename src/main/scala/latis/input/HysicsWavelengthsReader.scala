package latis.input

import latis.model._
import latis.metadata._
import java.net.URI
import latis.ops._
import latis.data._
import scala.io.Source

case class HysicsWavelengthsReader(uri: URI) extends AdaptedDatasetReader {
    
  // iw -> wavelength
  val model = Function(
    Scalar(Metadata("iw") + ("type" -> "int")),
    Scalar(Metadata("wavelength") + ("type" -> "double"))
  )
   
  override def metadata = Metadata(
    "id" -> "hysics_wavelengths"
  )
  
  /**
   * Use the matrix adapter but override it to keep just the first row.
   */
  def adapter: Adapter = new MatrixTextAdapter(TextAdapter.Config(), model) {
    override def apply(uri: URI): SampledFunction = super.apply(uri) match {
      case ArrayFunction2D(data2d) => ArrayFunction1D(data2d(0))
    }
  }
  
}