package latis.input

import latis.metadata._
import latis.model._


object GoesImageReader extends AdaptedDatasetReader {

  def metadata: Metadata = Metadata("goes_image") //TODO: make unique, from URI?

  def model: DataType = Function(
    Tuple(
      //Note, raw values are shorts but sclae and offset are applied
      Scalar(Metadata("y") + ("type" -> "float")),
      Scalar(Metadata("x") + ("type" -> "float"))
    ),
    Scalar(
      Metadata("radiance")
      + ("type" -> "float")
      + ("origName" -> "Rad")
    )
  )

  def adapter: Adapter = new NetcdfAdapter(model)

}
