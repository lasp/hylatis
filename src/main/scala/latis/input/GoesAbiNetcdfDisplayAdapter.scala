package latis.input

import java.net.URI
import java.awt.Color

import ucar.nc2._

import latis.data._
import latis.metadata._
import latis.model._

/**
 * Adapter for reading radiance data from GOES files on S3 or from local file system.
 * Implementation converts radiance values to interpolated colors.
 */
class GoesAbiNetcdfDisplayAdapter(model: Function) extends Adapter {
  val Shape: Int = 5424
  val ScaleFactor = 1          // full resolution
  //val ScaleFactor = 226      // shrink the number of points in each dimension by this multiplier
  //val ScaleFactor = 12
  //val ScaleFactor = 24
  //val ScaleFactor = 48
  //val ScaleFactor = 113
  //val ScaleFactor = 452
 
  val radianceColors = List(
    (0, new Color(255, 255, 255, 255)),
    (300, new Color(255, 0, 0, 255)),
    (400, new Color(0, 255, 0, 255)),
    (500, new Color(0, 0, 255, 255)),
    (1023, new Color(0, 0, 0, 255)))
  
  /**
   * The actual return type is IndexedFunction2D,
   * which extends Function which itself extends Data.
   */
  def apply(netCDFUri: URI): SampledFunction = {
    val netCDFFile: NetcdfFile = open(netCDFUri)
    val radianceVariable = netCDFFile.findVariable("Rad")
    val radianceData = radianceVariable.read
    netCDFFile.close()
    
    val as: Array[Any] = Array.range(0, Shape / ScaleFactor).map(x => x)
    val bs: Array[Any] = Array.range(0, Shape / ScaleFactor).map(x => x)
    val vs2d: Array[Array[Any]] = 
      Array.range(0, Shape / ScaleFactor) map { i => 
        Array.range(0, Shape / ScaleFactor) map { j => 
          val radiance: Int = radianceData.getInt((Shape * j  + i) * ScaleFactor)
          val colorCorrectedRadiance: Any = colorToInt(interpolateColor(radianceColors, radiance))
          colorCorrectedRadiance
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
  
  /**
   * Convert a RGB color object to an integer value.
   * For example a pure blue color RGB(0, 0, 255) = 255
   * A pure green color RGB(0, 255, 0) = 65,280
   * A pure red color RGB(255, 0, 0) = 16,711,680
   * The alpha channel must also be included.
   */
  def colorToInt(color: Color): Int = {
    color.getRGB
  }
  
  /**
   * Given two colors from a color table and a value between them, return the interpolated color.
   */
  def interpolateColorOfPair(pair: List[(Int, Color)], value: Int) : Option[Color] = {
    if (pair.head._1 > value || pair.last._1 <= value) None
    else {
      val fraction: Double = (value.toDouble - pair.head._1) / (pair.last._1 - pair.head._1)
      val deltaRed = ((pair.last._2.getRed - pair.head._2.getRed) * fraction).round.toInt
      val deltaGreen = ((pair.last._2.getGreen - pair.head._2.getGreen) * fraction).round.toInt
      val deltaBlue = ((pair.last._2.getBlue - pair.head._2.getBlue) * fraction).round.toInt
      Some(new Color(pair.head._2.getRed + deltaRed,
                pair.head._2.getGreen + deltaGreen,
                pair.head._2.getBlue + deltaBlue,
                255))
    }
  }
  
  /**
   * Given a specified color table and a value within the range of the color table, return a color.
   */
  def interpolateColor(colors: List[(Int, Color)], value: Int): Color = {
    val sortedColors: List[(Int, Color)] = colors.sortBy(_._1)
    if ( value <= sortedColors.head._1 ) sortedColors.head._2
    else if ( value >= sortedColors.last._1 ) sortedColors.last._2
    else {
      // Create a list of (Int, Color) pairs
      val pairs: List[List[(Int, Color)]] = sortedColors.sliding(2).toList
      val enclosingPair = pairs.find(x => x.head._1 <= value && x.last._1 > value)
      enclosingPair match {
        case Some(pair: List[(Int, Color)]) => 
          interpolateColorOfPair(pair, value).getOrElse(new Color(255, 255, 255, 255))
        case _ => new Color(0, 0, 0, 255)    // should be unreachable
      }
    }
  }
}
