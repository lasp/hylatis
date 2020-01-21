package latis.output

import latis.model._
import latis.data._
import latis.util.StreamUtils._
import scala.collection._
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.awt.Color
import java.io.OutputStream
import java.io.FileOutputStream
import latis.dataset.Dataset

/**
 * Create an png image of a Dataset of one of the shapes:
 *   (row, col) -> (red, green, blue)
 *   (row, col) -> RGBpackedInt
 * Note that this uses row-major ordering starting with
 * the first row at the top.
 */
class ImageWriter(out: OutputStream, format: String) { //extends Writer(out) {
  //TODO: work with http4s encoder idioms
  /*
   * TODO: define an Image type to enforce that it works with this writer
   * deal with non row-major ordering
   */
  //TODO: see other image types: https://docs.oracle.com/javase/7/docs/api/java/awt/image/BufferedImage.html

  def write(dataset: Dataset): Unit = {
    // Construct a BufferedImage based on the shape of the data
    val image: BufferedImage = ImageWriter.encode(dataset)
    // Write image to the OutputStream in the desired format
    ImageIO.write(image, format, out)
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
  
  
  def encode(dataset: Dataset): BufferedImage = dataset.model match {
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
  
  /**
   * Make an Image assuming a Dataset of the form: (x,y) -> color
   */
  private def makeImageFromPackedColor(dataset: Dataset): BufferedImage = {
    //collect set of row and col values only so we can count them
    //Sets only keep unique values
    val rows = mutable.Set[Any]()
    val cols = mutable.Set[Any]()
    val buffer = mutable.ArrayBuffer[Int]()
    unsafeStreamToSeq(dataset.samples) foreach {
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
   * with row-major ordering.
   */
  private def makeImageFromRGB0(dataset: Dataset): BufferedImage = {
    val rows = mutable.Set[Any]()
    val cols = mutable.Set[Any]()
    val rb = mutable.ArrayBuffer[Double]()
    val gb = mutable.ArrayBuffer[Double]()
    val bb = mutable.ArrayBuffer[Double]()
    
    // Make sense of a Seq of Samples.
    //TODO: take advantage of ArrayFunction2D and such
    unsafeStreamToSeq(dataset.samples) foreach {
      // Assumes Cartesian domain to determine the size of the image
      case Sample(DomainData(row, col), RangeData(Number(r), Number(g), Number(b))) =>
        rows += row
        cols += col
        rb += r
        gb += g
        bb += b
    }

    // Filter out NaN before finding min/max
    val rmax = rb.filter(! _.isNaN()).max
    val gmax = gb.filter(! _.isNaN()).max
    val bmax = bb.filter(! _.isNaN()).max
    val rmin = rb.filter(! _.isNaN()).min
    val gmin = gb.filter(! _.isNaN()).min
    val bmin = bb.filter(! _.isNaN()).min

    val width = cols.size
    val height = rows.size
    
    // Normalize to 0..1 based on range of min to max value.
    //TODO: make histogram and drop outer n%
    val data = for {
      row <- (0 until height)
      col <- (0 until width)
    } yield {
      val i = row * width + col
      val r = {
        val r = ((rb(i) - rmin) / (rmax - rmin)).toFloat
        if (r.isNaN) 0 else r
      }
      val g = {
        val g = ((gb(i) - gmin) / (gmax - gmin)).toFloat
        if (g.isNaN) 0 else g
      }
      val b = {
        val b = ((bb(i) - bmin) / (bmax - bmin)).toFloat
        if (b.isNaN) 0 else b
      }
      new Color(r, g, b).getRGB
    }    
    
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    image.setRGB(0, 0, width, height, data.toArray, 0, width)
    image
  }

  /**
   * Make an Image assuming a Dataset of the form: (x, y) -> (r, g, b)
   * with natural ordering.
   */
  private def makeImageFromRGB(dataset: Dataset): BufferedImage = {
    val xs = mutable.Set[Any]()
    val ys = mutable.Set[Any]()
    val rb = mutable.ArrayBuffer[Double]()
    val gb = mutable.ArrayBuffer[Double]()
    val bb = mutable.ArrayBuffer[Double]()
    
    // Make sense of a Seq of Samples.
    //TODO: take advantage of ArrayFunction2D and such
    unsafeStreamToSeq(dataset.samples) foreach {
      // Assumes Cartesian domain to determine the size of the image
      case Sample(DomainData(x, y), RangeData(Number(r), Number(g), Number(b))) =>
        xs += x
        ys += y
        rb += r
        gb += g
        bb += b
    }

    // Filter out NaN before finding min/max
    //val drop = 0.1
    val rmax = rb.filter(! _.isNaN()).max //* (1 - drop - 0.5)
    val gmax = gb.filter(! _.isNaN()).max //* (1 - drop - 0.5)
    val bmax = bb.filter(! _.isNaN()).max //* (1 - drop - 0.5)
    val rmin = rb.filter(! _.isNaN()).min //* (1 + drop)
    val gmin = gb.filter(! _.isNaN()).min //* (1 + drop)
    val bmin = bb.filter(! _.isNaN()).min //* (1 + drop)

    val width = xs.size
    val height = ys.size
    
    // Normalize to 0..1 based on range of min to max value.
    /*
     * TODO: make histogram and drop outer n%
     * try drop above
     */
    
    
    // Assume natural x-y ordering
    // Put into row-column order.
    val data = for {
      row <- (0 until height)  //height - y -1
      col <- (0 until width)   //x
    } yield {
      val i = col * height + height - row -1
      val r: Float = {
        ((rb(i) - rmin) / (rmax - rmin)).toFloat match {
          case v if v.isNaN => 0
          case v if v < 0   => 0
          case v if v > 1   => 1
          case v            => v
        }
      }
      val g: Float = {
        ((gb(i) - gmin) / (gmax - gmin)).toFloat match {
          case v if v.isNaN => 0
          case v if v < 0   => 0
          case v if v > 1   => 1
          case v            => v
        }
      }
      val b: Float = {
        ((bb(i) - bmin) / (bmax - bmin)).toFloat match {
          case v if v.isNaN => 0
          case v if v < 0   => 0
          case v if v > 1   => 1
          case v            => v
        }
      }
      new Color(r, g, b).getRGB
    }
    
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    image.setRGB(0, 0, width, height, data.toArray, 0, width)
    image
  }

}
