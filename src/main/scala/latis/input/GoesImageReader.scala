package latis.input

import latis.metadata._
import latis.model._
import java.net.URI


case class GoesImageReader(uri: URI) extends AdaptedDatasetReader {
   
  def model = Function(
    Tuple(
      Scalar(Metadata("y") + ("type" -> "short")), 
      Scalar(Metadata("x") + ("type" -> "short"))
    ),
    Scalar(
      Metadata("radiance")
      + ("type" -> "short")
      + ("origName" -> "Rad")
    )
  )

  def adapter: Adapter = new NetcdfAdapter(model)

  def metadata: Metadata = Metadata("goes_image")
}
