
 package latis.input

import latis.metadata.Metadata
import latis.model._

import java.net.URI

/**
 * The Hysics dataset consists of 4200 "image" text files each with
 * radiance in the "slit" and wavelength dimwnsion.
 */
case class HysicsImageReader(uri: URI) extends AdaptedDatasetReader {
   //TODO: replace with fdml
  
   // (ix, iw) -> radiance
  def model = Function(
    Tuple(
      Scalar(Metadata("ix") + ("type" -> "int")), 
      Scalar(Metadata("wavelength") + ("type" -> "double"))
    ),
    Scalar(Metadata("radiance") + ("type" -> "double"))
  )

  /**
   * Use the MatrixTextAdapter to read each "slit" image.
   */
  def adapter: Adapter = new MatrixTextAdapter(model, TextAdapter.Config())

}
