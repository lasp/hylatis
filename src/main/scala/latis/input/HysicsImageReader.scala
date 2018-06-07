package latis.input

import latis.ops._
import latis.data._
import latis.metadata._
import java.net.URI
import latis.util.HysicsUtils


class HysicsImageReader extends AdaptedDatasetSource {
  
  def uri: URI = ???

  // (y, x, wavelength) -> irradiance
  val model = FunctionType("f")(
    TupleType("")(
      ScalarType("y"),
      ScalarType("x"),
      ScalarType("wavelength")
    ),
    ScalarType("irradiance")
  )
  
  val metadata = Metadata("id" -> "hysics")(model)
  
  def adapter: Adapter = new Adapter {
    def apply(uri: URI): Data = {
      // Read image file and transform to x-y space; TODO: factor out operation
//      MatrixTextReader(uri).getDataset().samples map {
//        case Sample(_, Seq(Index(ix), Index(iw), Text(v))) =>
//          val (x, y) = HysicsUtils.indexToXY((ix, iy))
//          Sample(3, Real(y), Real(x), Real(wavelengths(iw)), Real(v.toDouble))
//        }
      ???
    }
  }
  
  /**
   * Add a coordinate system transformation.
   */
  //TODO: override def processingInstructions: Seq[Operation]

}

object HysicsImageAdapter {
  def apply() = new HysicsImageReader()
}