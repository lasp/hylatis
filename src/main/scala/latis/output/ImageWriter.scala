package latis.output

import latis.model._
import latis.data._
import scala.collection._
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.io.File
import java.awt.Color
import java.io.OutputStream
import java.io.FileOutputStream
import latis.metadata._

class ImageWriter(out: OutputStream, format: String) extends Writer(out) {
  //TODO: add properties, WriterProperties that looks for and hides the "writer" part
  //TODO: define an Image type to enforce that it works with this writer, at least Cartesian
  //      support the various forms with transformations
  //TODO: enumerate format options, not clearly defined
  //TODO: see other image types: https://docs.oracle.com/javase/7/docs/api/java/awt/image/BufferedImage.html

  override def write(dataset: Dataset): Unit = {
    // Construct a BufferedImage based on the shape of the data
    val image: BufferedImage = dataset.model match {
      //assert that domain arity is 2; TODO: assert Cartesian
      //  domain: (x,y) //TODO: x -> y
      //  range: (r,g,b) or packed color
      case Function(domain, range) => domain match {
        case Tuple(_, _) => range match {
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
    dataset.samples foreach {
      case (DomainData(row, col), RangeData(v: Double)) => //TODO: don't assume Double, extract double with Number match?
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
   * Make an Image assuming a Dataset of the form: (x,y) -> (r, g, b)
   */
  private def makeImageFromRGB(dataset: Dataset): BufferedImage = {
    val rows = mutable.Set[Any]()
    val cols = mutable.Set[Any]()
    val rb = mutable.ArrayBuffer[Double]()
    val gb = mutable.ArrayBuffer[Double]()
    val bb = mutable.ArrayBuffer[Double]()
    dataset.samples foreach {
      case (DomainData(row, col), RangeData(r: String, g: String, b: String)) => //TODO: extract double with Number match?
        rows += row
        cols += col
        rb += Math.max(0, r.toDouble)
        gb += Math.max(0, g.toDouble)
        bb += Math.max(0, b.toDouble)
    }

    val width = cols.size
    val height = rows.size
    val rmax = rb.max
    val gmax = gb.max
    val bmax = bb.max

    //normalize to 0..1
    val data = for {
      row <- (0 until height)
      col <- (0 until width)
    } yield {
      val i = row * width + col
      val r = (rb(i) / rmax).toFloat
      val g = (gb(i) / gmax).toFloat
      val b = (bb(i) / bmax).toFloat
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
