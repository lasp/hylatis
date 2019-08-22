package latis.input

import java.net.URI

import ucar.nc2._

import latis.data._
import ucar.ma2.Section
import latis.util.LatisConfig

/**
 * Adapter for reading radiance data from GOES files on S3 or from local file system.
 * This dataset has row-major ordering from the north. Use row-col domain ordering.
 */
case class GoesNetcdfAdapter() extends Adapter {
  val Shape: Int = 5424 //TODO: get from NetCDF file dimensions
  val scaleFactor: Int = LatisConfig.getOrElse("hylatis.goes.scale-factor", 1)
  val scaledShape = Shape / scaleFactor
  
  /**
   * The actual return type is IndexedFunction2D,
   * which extends Function which itself extends Data.
   */
  def apply(netCDFUri: URI): SampledFunction = {
    val netCDFFile: NetcdfFile = open(netCDFUri)
    val radianceVariable = netCDFFile.findVariable("Rad") 
    val section = new Section(s"(0:5423:$scaleFactor, 0:5423:$scaleFactor)")
    val radianceData = radianceVariable.read(section) //2D short array
    netCDFFile.close()
    def getRadiance(row: Int, col: Int) = RangeData(radianceData.getInt(scaledShape * row + col))
    //TODO: try to suck in blob?
    val vs2d: Array[Array[RangeData]] = Array.tabulate(scaledShape, scaledShape)(getRadiance)

    GoesArrayFunction2D(vs2d) // has coordinate system transform built in
  }
  
  /**
    * Return a NetcdfFile
    */
  def open(uri: URI): NetcdfFile = {
    uri.getScheme match {
      case null => 
        NetcdfFile.open(uri.getPath) //assume file path
//      case "s3" => 
//        val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
//        val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
//        NetcdfFile.open(raf, uriExpression, null, null)
      //TODO:  "file"
      case _    =>
        NetcdfFile.open(uri.getScheme + "://" + uri.getHost + "/" + uri.getPath)
    }
//    if (uri.getScheme.startsWith("s3")) {
//      val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
//      val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
//      NetcdfFile.open(raf, uriExpression, null, null)
//    } else {
//      NetcdfFile.open(uri.getScheme + "://" + uri.getHost + "/" + uri.getPath)
//    }
  }
  
}
