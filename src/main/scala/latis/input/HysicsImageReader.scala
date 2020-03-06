package latis.input

import latis.metadata.Metadata
import latis.model._

/**
 * Defines a reader for a Hyscis data granule.
 */
object HysicsImageReader extends AdaptedDatasetReader with Serializable {

  def metadata: Metadata = Metadata("tmp_image")

   // (iy, iw) -> radiance
  def model = Function(
    Tuple(
      Scalar(Metadata("iy") + ("type" -> "int")),
      Scalar(Metadata("iw") + ("type" -> "int"))
    ),
    Scalar(Metadata("radiance") + ("type" -> "double"))
  )

  // Use the MatrixTextAdapter to read each "slit" image.
  def adapter: Adapter = new MatrixTextAdapter(model)

}
