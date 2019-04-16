package latis.input

import latis.metadata._
import latis.model._
import java.net.URI


case class GoesImageReader(uri: URI) extends AdaptedDatasetReader {
   
  def model = Function(
    Tuple(
      Scalar(Metadata("iy") + ("type" -> "int")), 
      Scalar(Metadata("ix") + ("type" -> "int"))
    ),
    Scalar(Metadata("radiance") + ("type" -> "double"))
  )


  def adapter: Adapter = new GoesNetcdfAdapter()

}
