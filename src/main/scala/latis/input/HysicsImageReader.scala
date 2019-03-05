
 package latis.input

import latis.ops._
import latis.data._
import latis.metadata._
import latis.model._
import java.net.URI
import latis.util.HysicsUtils


case class HysicsImageReader(uri: URI) extends AdaptedDatasetReader {
   //TODO: replace with fdml

//  // (y, x, wavelength) -> irradiance
//  val model = FunctionType(
//    TupleType(
//      ScalarType("y"),
//      ScalarType("x"),
//      ScalarType("wavelength")
//    ),
//    ScalarType("irradiance")
//  )
  
   // (ix, iw) -> irradiance
  def model = Function(
    Tuple(
      Scalar(Metadata("ix") + ("type" -> "int")), 
      Scalar(Metadata("wavelength") + ("type" -> "double"))
    ),
    Scalar(Metadata("irradiance") + ("type" -> "double"))
  )


  def adapter: Adapter = new MatrixTextAdapter(TextAdapter.Config(), model)
  

//  override def processingInstructions: Seq[Operation] = {
//    val cst = new MapOperation {
//      def makeMapFunction(model: DataType): Sample => Sample = {
////        (s: Sample) => s match {
////          case Sample(_, Seq(Index(ix), Index(iw), Text(v))) =>
////            val (x, y) = HysicsUtils.indexToXY((ix, iy))
////            Sample(3, Real(y), Real(x), Real(wavelengths(iw)), Real(v.toDouble))
////        }
//        /*
//         * TODO: need iy and wavelengths
//         * only provide (ix, iw) -> f here
//         * might as well just use MatrixReader in parent
//         * 
//         * consider granule join
//         * separation of concerns
//         * do we need full datasets or just data (adapter) from granules
//         * would be nice to do a true join of granule datasets
//         * we should use a DatasetSource to get the granule dataset
//         *   that could be a FdmlReader
//         *   in this case a MatrixReader: (row, col) -> value
//         * join to get index -> (row, col) -> value
//         *   uncurry, rename
//         *   substitute? wavelengths
//         *   cs transform
//         *   
//         * JoinedDatasetSource
//         *   join: BinaryOperation
//         *   model: join.applyToModel
//         *   metadata: specify at the joined level
//         * GranuleJoin
//         *   
//         * 
//         * GranuleList dataset
//         * 
//         */
//        ???
//      }
//    }
//    
//    List(cst)
//  }

}

//object HysicsImageAdapter {
//  def apply() = new HysicsImageReader()
//}