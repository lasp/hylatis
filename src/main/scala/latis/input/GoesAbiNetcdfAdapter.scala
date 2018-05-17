package latis.input

import java.net.URI

import latis.data._
import latis.metadata._

import java.awt.Color

import ucar.nc2._

/**
 * Adapter for reading radiance data from GOES files on S3 or from local file system.
 * Current implementation converts radiance values to interpolated colors.
 */
class GoesAbiNetcdfAdapter(model: FunctionType) extends Adapter {
  val Shape: Int = 5424
  
  val radianceColors = List(
    (0, new Color(255, 255, 255)),
    (300, new Color(255, 0, 0)),
    (400, new Color(0, 255, 0)),
    (500, new Color(0, 0, 255)),
    (1023, new Color(0, 0, 0)))
  
  /**
   * The actual return type is Function2D,
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
          val colorCorrectedRadiance = colorToInt(interpolateColor(radianceColors, radiance))
          val rad: Data = Integer(colorCorrectedRadiance)
          rad  
        }
    }

    new IndexedFunction2D(as, bs, vs2d)
  }
  
  /**
    * Return a NetcdfFile
    */
  def open(uri: URI): NetcdfFile = {
    val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
    //if (uri.getScheme.startsWith("s3:")) {
    //  val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
    //  NetcdfFile.open(raf, uriExpression, null, null)
    //} else {
      NetcdfFile.open(uri.getScheme + "://" + uri.getPath)
    //}
  }
  
  /**
   * Convert a RGB color object to an integer value.
   * For example a pure blue color RGB(0, 0, 255) = 255
   * A pure green color RGB(0, 255, 0) = 65,280
   * A pure red color RGB(255, 0, 0) = 16,711,680
   */
  def colorToInt(color: Color): Int = {
    (color.getRed & 0xFF)<<16 | (color.getGreen & 0xFF) << 8  | (color.getBlue & 0xFF)
  }
  
  /**
   * Given two colors from a color table and a value between them, return the interpolated color.
   */
  def interpolateColorOfPair(pair: List[(Int, Color)], value: Int) : Option[Color] = {
    if (pair.head._1 <= value && pair.last._1 > value) None
    else {
      val fraction: Double = (value.toDouble - pair.head._1) / (pair.last._1 - pair.head._1)
      val deltaRed = ((pair.last._2.getRed - pair.head._2.getRed) * fraction).round.toInt
      val deltaGreen = ((pair.last._2.getGreen - pair.head._2.getGreen) * fraction).round.toInt
      val deltaBlue = ((pair.last._2.getBlue - pair.head._2.getBlue) * fraction).round.toInt
      Some(new Color(pair.head._2.getRed + deltaRed,
                pair.head._2.getGreen + deltaGreen,
                pair.head._2.getBlue + deltaBlue))
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
          interpolateColorOfPair(pair, value).getOrElse(new Color(255, 255, 255))
        case _ => new Color(0, 0, 0)    // should be unreachable
      }
    }
  }
}
