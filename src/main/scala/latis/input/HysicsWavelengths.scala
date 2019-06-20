package latis.input

import latis.model._
import latis.metadata._
import latis.data._
import latis.util.LatisConfig
import java.net.URI
import java.net.URI

class HysicsWavelengths extends DatasetResolver {
  //TODO: factor out superclass that tests companion object id?
  
  /**
   * Support the DatasetResolver interface.
   */
  def getDataset(id: String): Option[Dataset] = {
    if (id == HysicsWavelengths.id) Option(HysicsWavelengths())
    else None
  }
}

object HysicsWavelengths {
  
  val id = "hysics_wavelengths"
  
  /**
   * Construct a Dataset for the Hysics wavelength data.
   *   iw -> wavelength
   */
  def apply(): Dataset = {
    val metadata = Metadata("id" -> id)
    
    // iw -> wavelength
    val model = Function(
      Scalar(Metadata("iw") + ("type" -> "int")),
      Scalar(Metadata("wavelength") + ("type" -> "double")))

    def notFound = throw new RuntimeException("hylatis.hysics.base-uri not defined")
    //val defaultBase = "s3://hylatis-hysics-001/des_veg_cloud"
    val base = LatisConfig.get("hylatis.hysics.base-uri").getOrElse(notFound)
    val uri = new URI(s"$base/wavelength.txt")

    val data = new MatrixTextAdapter(model, TextAdapter.Config())(uri) match {
      case ArrayFunction2D(data2d) => ArrayFunction1D(data2d(0))
    }
    
    Dataset(metadata, model, data)
  }
}
