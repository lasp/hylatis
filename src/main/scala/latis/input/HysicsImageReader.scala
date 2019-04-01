
 package latis.input

import latis.metadata.Metadata
import latis.model._

import java.net.URI

/**
 * The Hysics dataset consists of 4200 "image" text files each with
 * irradiance in the "slit" and wavelength dimwnsion.
 */
case class HysicsImageReader(uri: URI) extends AdaptedDatasetReader {
   //TODO: replace with fdml
  
   // (ix, iw) -> irradiance
  def model = Function(
    Tuple(
      Scalar(Metadata("ix") + ("type" -> "int")), 
      Scalar(Metadata("wavelength") + ("type" -> "double"))
    ),
    Scalar(Metadata("irradiance") + ("type" -> "double"))
  )

  /**
   * Use the MatrixTextAdapter to read each "slit" image.
   */
  def adapter: Adapter = new MatrixTextAdapter(TextAdapter.Config(), model)

}
