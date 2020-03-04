package latis.util

import java.awt.Color

import scala.util.Random.shuffle

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.util.ImageUtils._

class ImageUtilsSpec extends FlatSpec {

  private val data = Array[Double](
    0.376, 0.864, 0.512, 0.434, 0.411, 0.225, 0.806, 0.941, 1.058, 0.453, 0.628, 0.58, 1.047, 0.532,
    0.625, 0.825, 0.368, 0.43, 0.57, 0.39
  )

  "rescale" should "rescale a sequence" in {
    val scaledData = rescale(data, (data.min, data.max), (0.0, 100.0))
    scaledData.min should be(0.0 +- 0.0001)
    scaledData.max should be(100.0 +- 0.0001)
  }

  "linearPercentStretch" should "clip extreme values of a sequence and stretch the rest" in {
    val stretchedData = linearPercentStretch(data, 0.03)

    stretchedData.min should be(0.0 +- 0.0001)
    stretchedData.max should be(1.0 +- 0.0001)
  }

  "linearPercentStretchRgb" should "clip extreme values of a sequence of RGB values and stretch the rest" in {
    // make a list that spans a little more than data does
    val otherData     = data.map(x => (x - 0.5) * 10.0).toList
    val rs            = shuffle(otherData)
    val gs            = shuffle(otherData)
    val bs            = shuffle(otherData)
    val rgbData       = (rs, gs, bs).zipped.toList
    val stretchedData = linearPercentStretchRgb(rgbData, 0.03)
    // Not sure of a good way to test this
    for ((r, g, b) <- stretchedData) {
      val a = Array(r, g, b)
      // check that all values are between 0 and 1
      a.min should be >= 0.0
      a.max should be <= 1.0
    }
  }

  "rescaleToByte" should "rescale a sequence to byte values" in {
    val scaledData = rescaleToByte(data)

    scaledData.min should be(Byte.MinValue)
    scaledData.max should be(Byte.MaxValue)
  }

  "rescaleRgbToByte" should "rescale a sequence of RGB to byte vales" in {
    val rgb: Seq[(Double, Double, Double)] = List((1, 0, 0), (0, 1, 0), (0, 0, 1), (1, 1, 1))
    val byteData                           = rescaleRgbToByte(rgb)
    val expected: Seq[(Byte, Byte, Byte)] = List(
      (127, -128, -128),
      (-128, 127, -128),
      (-128, -128, 127),
      (127, 127, 127)
    )

    byteData should be(expected)
  }

  "toJavaColor" should "convert a sequence of RGB values to java.awt.Color objects" in {
    val rgb: Seq[(Double, Double, Double)] = List((1, 0, 0), (0, 1, 0), (0, 0, 1), (1, 1, 1))
    val colors                             = toJavaColor(rgb)
    val expected                           = List(Color.red, Color.green, Color.blue, Color.white)

    colors should be(expected)
  }

}
