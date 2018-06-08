package latis.input

import java.net.URI
import java.awt.Color

import ucar.nc2._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import latis.data._
import latis.metadata._

/**
 * Adapter for reading radiance data from GOES files on S3 or from local file system.
 */
class GoesAbiNetcdfAdapter(model: FunctionType) extends Adapter {
  val Shape: Int = 5424
  
  val logger = LoggerFactory.getLogger("GoesAbiNetcdfAdapter")
  
  /**
   * The actual return type is IndexedFunction2D,
   * which extends Function which itself extends Data.
   */
  def apply(netCDFUri: URI): Data = {
    val netCDFFile: NetcdfFile = open(netCDFUri)
    val radianceVariable = netCDFFile.findVariable("Rad")
    val radianceData = radianceVariable.read
    netCDFFile.close()
    
    val as: Array[Data] = Array.range(0, Shape).map(Integer(_))
    val bs: Array[Data] = Array.range(0, Shape).map(Integer(_))
    val vs2d: Array[Array[Data]] = 
      Array.range(0, Shape) map { i => 
        Array.range(0, Shape) map { j => 
          val radiance = radianceData.getInt(Shape * j + i)
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
    val logger = LoggerFactory.getLogger("Open")
    if (uri.getScheme.startsWith("s3")) {
      val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
      val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
      println("   raf: " + raf)
      NetcdfFile.open(raf, uriExpression, null, null)
    } else {
      NetcdfFile.open(uri.getScheme + "://" + uri.getPath)
    }
  }
  
}
