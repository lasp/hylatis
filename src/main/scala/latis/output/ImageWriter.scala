package latis.output

import latis.model._
import latis.data._
import latis.util.StreamUtils._
import scala.collection._
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.io.File
import java.awt.Color
import java.io.OutputStream
import java.io.FileOutputStream
import latis.metadata._
import org.apache.spark.sql.catalyst.expressions.IsNaN

/**
 * Create an png image of a Dataset of one of the shapes:
 *   (row, col) -> (red, green, blue)
 *   (row, col) -> RGBpackedInt
 * Note that this uses row-major ordering starting with
 * the first row at the top.
 */
class ImageWriter(out: OutputStream, format: String) { //extends Writer(out) {
  //TODO: define an Image type to enforce that it works with this writer
  //TODO: see other image types: https://docs.oracle.com/javase/7/docs/api/java/awt/image/BufferedImage.html

  def write(dataset: Dataset): Unit = {
    // Construct a BufferedImage based on the shape of the data
    val image: BufferedImage = dataset.model match {
      case Function(domain, range) => domain match {
        case Tuple(_, _) => range match { // 2D
          case _: Scalar      => makeImageFromPackedColor(dataset)
          case Tuple(_, _, _) => makeImageFromRGB(dataset)
          case _                  => ??? //TODO: invalid range type
        }
        case _ => ??? //TODO: invalid domain type
      }
      case _ => ??? //TODO: invalid data type
    }
    
    ImageIO.write(image, format, out)
  }
  
  /**
   * Make an Image assuming a Dataset of the form: (x,y) -> color
   */
  private def makeImageFromPackedColor(dataset: Dataset): BufferedImage = {
    //collect set of row and col values only so we can count them
    //Sets only keep unique values
    val rows = mutable.Set[Any]()
    val cols = mutable.Set[Any]()
    val buffer = mutable.ArrayBuffer[Int]()
    unsafeStreamToSeq(dataset.data.streamSamples) foreach {
      case Sample(DomainData(row, col), RangeData(Number(v))) =>
        rows += row
        cols += col
        buffer += v.toInt 
    }

    val width = cols.size
    val height = rows.size
    val data = buffer.toArray
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    image.setRGB(0, 0, width, height, data, 0, width)
    image
  }

  /**
   * Make an Image assuming a Dataset of the form: (row, col) -> (r, g, b)
   */
  private def makeImageFromRGB(dataset: Dataset): BufferedImage = {
    val rows = mutable.Set[Any]()
    val cols = mutable.Set[Any]()
    val rb = mutable.ArrayBuffer[Double]()
    val gb = mutable.ArrayBuffer[Double]()
    val bb = mutable.ArrayBuffer[Double]()
    
    // Make sense of a Seq of Samples.
    //TODO: take advantage of ArrayFunction2D and such
    unsafeStreamToSeq(dataset.data.streamSamples) foreach {
      // Assumes Cartesian domain to determine the size of the image
      case Sample(DomainData(row, col), RangeData(Number(r), Number(g), Number(b))) =>
        rows += row
        cols += col
        rb += (if (r.isNaN) 0 else r)
        gb += (if (g.isNaN) 0 else g)
        bb += (if (b.isNaN) 0 else b)
    }

    val width = cols.size
    val height = rows.size
    val rmax = rb.max
    val gmax = gb.max
    val bmax = bb.max
    val rmin = rb.min
    val gmin = gb.min
    val bmin = bb.min

    // Normalize to 0..1 based on range of min to max value.
    //TODO: make histogram and drop outer n%
    val data = for {
      row <- (0 until height)
      col <- (0 until width)
    } yield {
      val i = row * width + col
      val r = ((rb(i) - rmin) / (rmax - rmin)).toFloat
      val g = ((gb(i) - gmin) / (gmax - gmin)).toFloat
      val b = ((bb(i) - bmin) / (bmax - bmin)).toFloat
      new Color(r, g, b).getRGB
    }
    
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    image.setRGB(0, 0, width, height, data.toArray, 0, width)
    image
  }

}


object ImageWriter {
  //Available Java ImageIO writer plug-ins: JPEG, PNG, GIF, BMP and WBMP, +
  //JAI has more; jpeg2000?
  
  def apply(out: OutputStream, format: String): ImageWriter = 
    new ImageWriter(out, format)
  
  def apply(file: String, format: String): ImageWriter = 
    ImageWriter(new FileOutputStream(file), format)
    
  def apply(file: String): ImageWriter = {
    val format = file.substring(file.lastIndexOf(".") + 1) //file suffix //TODO: util function
    ImageWriter(new FileOutputStream(file), format)
  }
}
