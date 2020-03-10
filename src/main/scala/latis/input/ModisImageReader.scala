package latis.input

import latis.metadata.Metadata
import latis.model._

object ModisImageReader extends AdaptedDatasetReader with Serializable {

  def metadata: Metadata = Metadata("modis_image")

  def model = Function(
    Tuple(
      Scalar(Metadata("id" -> "ix", "type" -> "int")),
      Scalar(Metadata("id" -> "iy", "type" -> "int"))
    ),
    Scalar(Metadata("id" -> "radiance", "type" -> "float"))
  )

  def adapter: Adapter = ModisNetcdfAdapter(model)

}
