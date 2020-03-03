package latis.input

import java.net.URI

import latis.data._
import latis.dataset._
import latis.metadata._
import latis.model._
import latis.util.LatisConfig

object HysicsWavelengthReader extends DatasetReader {
  /*
  TODO: wavelength ordering
     wavelengths are in reverse order in dataset
     but wavelength itself is naturally ordered
     should the order property be in scalar or function metadata?
     note that it is actually the index variable in the domain in this case
     could this work if the index var had desc order?
   */

  // iw -> wavelength
  def model: DataType = Function(
    Scalar(Metadata(
      "id" -> "iw",
      "type" -> "int"
    )),
    Scalar(Metadata(
      "id" -> "wavelength",
      "type" -> "double"
    ))
  )

  def read(uri: URI): Dataset = {
    def metadata: Metadata = Metadata("hysics_wavelengths")

    val data = MatrixTextAdapter(model).getData(uri) match {
      case ArrayFunction2D(data2d) => ArrayFunction1D(data2d(0))
    }

    new MemoizedDataset(metadata, model, data)
  }

}
