package latis.input

import java.net.URI

import latis.data._
import latis.dataset._
import latis.metadata._
import latis.model._
import latis.util.LatisConfig

class HysicsWavelengths extends StaticDatasetResolver {
  //TODO: make this a Reader since it can use diff URIs

  def datasetIdentifier: String = "hysics_wavelengths"
  
  /**
   * Construct a Dataset for the Hysics wavelength data.
   *   iw -> wavelength
   */
  def getDataset(): Dataset = {
    val metadata = Metadata("id" -> datasetIdentifier)
    
    // iw -> wavelength
    val model = Function(
      Scalar(Metadata("iw") + ("type" -> "int")),
      Scalar(Metadata("wavelength") + ("type" -> "double")))

    def notFound = throw new RuntimeException("hylatis.hysics.base-uri not defined")
    //val defaultBase = "s3://hylatis-hysics-001/des_veg_cloud"
    val base = LatisConfig.get("hylatis.hysics.base-uri").getOrElse(notFound)
    val uri = new URI(s"$base/wavelength.txt")

    val data = MatrixTextAdapter(model).getData(uri) match {
      case ArrayFunction2D(data2d) => ArrayFunction1D(data2d(0))
    }
    
    new MemoizedDataset(metadata, model, data)
  }
}
