package latis.util

import java.awt.Color

import scala.util.Sorting

object ImageUtils {

  private def percentile(it: TraversableOnce[Double], p: Double): Double = {
    if (p > 1 || p < 0) throw LatisException("p must be in [0,1]")
    val arr = it.toArray.clone()
    Sorting.quickSort(arr)
    percentileInPlace(arr, p)
  }

  private def percentileInPlace(arr: Array[Double], p: Double): Double = {
    if (p > 1 || p < 0) throw LatisException("p must be in [0,1]")
    val f = (arr.length - 1) * p
    val i = f.toInt
    if (i == arr.length - 1) arr.last
    else arr(i) + (f - i) * (arr(i + 1) - arr(i))
  }

  /**
   * Maps intensity values in `a` to new values such that values in `inRange` map to
   * values in `outRange`.
   *
   * @param a Sequence of image intensity values
   * @param inRange Min and max intensity values of input image.
   * @param outRange Min and max intensity values of output image.
   * @example
   * {{{
   *   val scaledImage = rescale(a, (a.min, a.max), (0.0, 1.0))
   * }}}
   */
  def rescale(
    a: Seq[Double],
    inRange: (Double, Double),
    outRange: (Double, Double)
  ): Seq[Double] = {
    val (in1, in2)   = inRange
    val (out1, out2) = outRange
    if (in2 - in1 == 0)
      Seq.fill(a.length)(out1) // Should we throw an exception here?
    else {
      val dilation = (out2 - out1) / (in2 - in1)
      def rescale_val(d: Double): Double =
        if (d < in1) out1
        else if (d > in2) out2
        else (d - in1) * dilation + out1
      a.map(rescale_val)
    }
  }

  /**
   * Saturates the lowest and highest `p` % of intensity values, then rescales
   * the values to be between `0` and `1`.
   * @param a Sequence of image intensity values
   * @param p Percentile to saturate
   * @throws `LatisException` if `p` is not in `[0, 0.5)`
   */
  def linearPercentStretch(a: Seq[Double], p: Double): Seq[Double] = {
    if (p >= 0.5 || p < 0) throw LatisException("p must be in [0,0.5)")
    val p1p2 = (percentile(a, 0.0 + p), percentile(a, 1.0 - p))
    rescale(a, p1p2, (0.0, 1.0))
  }

  // FIXME: This scales the three colors independently!
  /**
   * Applies `linearPercentStretch` to each color channel of input RGB sequence.
   * @param rgb Sequence of RGB tuples
   * @param p Percentile to saturate
   */
  def linearPercentStretchRgb(
    rgb: Seq[(Double, Double, Double)],
    p: Double
  ): Seq[(Double, Double, Double)] = {
    val f = linearPercentStretch(_: Seq[Double], p)
    mapFunctionToRgb(rgb, f)
  }

  /**
   * Takes a sequence of RGB tuples and a function `f` that acts on a sequence of
   * image intensity values, and applies `f` to each color channel.
   */
  private def mapFunctionToRgb[A, B](
    rgb: Seq[(A, A, A)],
    f: Seq[A] => Seq[B]
  ): Seq[(B, B, B)] = {
    val (rs, gs, bs) = rgb.unzip3
    (f(rs), f(gs), f(bs)).zipped.toList
  }

  /**
   * Rescales image intensity values to Scala `Byte` values.
   *
   * @param a Sequence of image intensity values
   * @note Byte is signed, so output values will be between -128 and 127, not 0 and 255.
   */
  def rescaleToByte(a: Seq[Double]): Seq[Byte] =
    rescale(a, (a.min, a.max), (Byte.MinValue, Byte.MaxValue)).map(_.toByte)

  // FIXME: This scales the three colors independently!
  /**
   * Rescales values of each color channel to Scala `Byte` values.
   *
   * @param rgb Sequence of RGB tuples
   * @note Byte is signed, so output values will be between -128 and 127, not 0 and 255.
   */
  def rescaleRgbToByte(rgb: Seq[(Double, Double, Double)]): Seq[(Byte, Byte, Byte)] =
    mapFunctionToRgb(rgb, rescaleToByte)

  // FIXME: This scales the three colors independently!
  /**
   * Rescales values of each color channel, then converts the RGB tuples to
   * [[https://docs.oracle.com/javase/7/docs/api/java/awt/Color.html java.awt.Color]] objects.
   *
   * @param rgb Sequence of RGB tuples
   */
  def toJavaColor(rgb: Seq[(Double, Double, Double)]): Seq[Color] = {
    def f(a: Seq[Double]): Seq[Int] = rescale(a, (a.min, a.max), (0, 255)).map(_.toInt)
    mapFunctionToRgb(rgb, f).map {
      case (r, g, b) => new Color(r, g, b)
    }
  }
}
