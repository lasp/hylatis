package latis.input

import java.net.URI
import java.awt.Color

import ucar.nc2._

import latis.data._
import latis.metadata._

/**
 * Adapter for reading radiance data from GOES files on S3 or from local file system.
 */
case class GoesAbiNetcdfAdapter() extends Adapter {
  val Shape: Int = 5424
  //val ScaleFactor = 1        // full resolution
  //val ScaleFactor = 226      // shrink the number of points in each dimension by this multiplier
  val ScaleFactor = 4
  //val ScaleFactor = 12
  //val ScaleFactor = 24
  //val ScaleFactor = 48
  //val ScaleFactor = 113
  //val ScaleFactor = 452
  
  /**
   * The actual return type is IndexedFunction2D,
   * which extends Function which itself extends Data.
   */
  def apply(netCDFUri: URI): Data = {
    val netCDFFile: NetcdfFile = open(netCDFUri)
    val radianceVariable = netCDFFile.findVariable("Rad")
    val radianceData = radianceVariable.read
    netCDFFile.close()
    
    val as: Array[Data] = Array.range(0, Shape / ScaleFactor).map(Integer(_))
    val bs: Array[Data] = Array.range(0, Shape / ScaleFactor).map(Integer(_))
    val vs2d: Array[Array[Data]] = 
      Array.range(0, Shape / ScaleFactor) map { i => 
        Array.range(0, Shape/ ScaleFactor) map { j => 
          val radiance = radianceData.getInt((Shape * j  + i) * ScaleFactor)
          val rad: Data = Integer(radiance)
          rad  
        }
    }

    new IndexedFunction2D(as, bs, vs2d)
  }
  
  /**
    * Return a NetcdfFile
    */
  def open(uri: URI): NetcdfFile = {
    if (uri.getScheme.startsWith("s3")) {
      val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
      val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
      NetcdfFile.open(raf, uriExpression, null, null)
    } else {
      NetcdfFile.open(uri.getScheme + "://" + uri.getPath)
    }
  }
  
}
