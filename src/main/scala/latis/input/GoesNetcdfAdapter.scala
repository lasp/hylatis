package latis.input

import java.net.URI

import ucar.nc2._

import latis.data._
import ucar.ma2.Section
import latis.util.LatisProperties

/**
 * Adapter for reading radiance data from GOES files on S3 or from local file system.
 */
case class GoesNetcdfAdapter() extends Adapter {
  val Shape: Int = 5424 //TODO: get from NetCDF file dimensions
  //val ScaleFactor = 1        // full resolution
  //val ScaleFactor = 226      // shrink the number of points in each dimension by this multiplier
  //val ScaleFactor = 4
  //val ScaleFactor = 6
  //val ScaleFactor = 12
  //val ScaleFactor = 24
  //val ScaleFactor = 48
  //val ScaleFactor = 113
  //val ScaleFactor = 452
  val scaleFactor: Int = LatisProperties.get("goes.scale.factor") match {
    case Some(s) => s.toInt
    case None => 1
  }
  val scaledShape = Shape / scaleFactor
  
  /**
   * The actual return type is IndexedFunction2D,
   * which extends Function which itself extends Data.
   */
  def apply(netCDFUri: URI): SampledFunction = {
    val netCDFFile: NetcdfFile = open(netCDFUri)
    val radianceVariable = netCDFFile.findVariable("Rad") 
    val section = new Section(s"(0:5423:$scaleFactor, 0:5423:$scaleFactor)")
    val radianceData = radianceVariable.read(section)
    netCDFFile.close()
    def getRadiance(i: Int, j: Int) = RangeData(radianceData.getInt(scaledShape * j  + i))
    val vs2d: Array[Array[RangeData]] = Array.tabulate(scaledShape, scaledShape)(getRadiance)

    ArrayFunction2D(vs2d)
  }
  
  /**
    * Return a NetcdfFile
    */
  def open(uri: URI): NetcdfFile = {
    uri.getScheme match {
      case null => 
        NetcdfFile.open(uri.getPath) //assume file path
      case "s3" => 
        val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
        val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
        NetcdfFile.open(raf, uriExpression, null, null)
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
