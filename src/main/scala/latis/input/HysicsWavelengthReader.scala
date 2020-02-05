package latis.input

import java.net.URI

import latis.data._
import latis.dataset._
import latis.metadata._
import latis.model._
import latis.util.LatisConfig

object HysicsWavelengthReader extends DatasetReader {

  // iw -> wavelength
  def model: DataType = Function(
    Scalar(Metadata("iw") + ("type" -> "int")),
    Scalar(Metadata("wavelength") + ("type" -> "double"))
  )

  def read(uri: URI): Dataset = {
    def metadata: Metadata = Metadata("hysics_wavelengths")

    val data = MatrixTextAdapter(model).getData(uri) match {
      case ArrayFunction2D(data2d) => ArrayFunction1D(data2d(0))
    }

    new MemoizedDataset(metadata, model, data)
  }

}