package latis.input

import latis.model._
import latis.metadata._
import java.net.URI
import latis.ops._
import latis.data._
import scala.io.Source

case class HysicsWavelengthsReader(uri: URI) extends AdaptedDatasetSource {
    
  // iw -> wavelength
  val model = Function(
    Scalar(Metadata("iw") + ("type" -> "int")),
    Scalar(Metadata("wavelength") + ("type" -> "double"))
  )
   
  override def metadata = Metadata(
    "id" -> "hysics_wavelengths"
  )
  
  override def operations: Seq[UnaryOperation] = Seq(
    //Projection("iw","wavelength") //TODO: need to replace "row" with index
      /*
       * TODO: slice (partially evaluate) at row=0
       * implement BijectiveFunction?
       */
  )
    
  def adapter: Adapter = new Adapter() {
    def apply(uri: URI): SampledFunction = {
      val is = uri.toURL.openStream
      val source = Source.fromInputStream(is)
      val data = source.getLines().next.split(",").map(s => RangeData(s.toDouble))
      source.close
      ArrayFunction1D(data)
    }
  }
  //new MatrixTextAdapter(TextAdapter.Config(), model)
}