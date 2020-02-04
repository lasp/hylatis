package latis

import java.io.OutputStream

import almond.display.Image
import org.checkerframework.checker.units.qual.g

import latis.data.BinSet2D
import latis.data.RddFunction
import latis.data.SeqFunction
import latis.dataset.Dataset
import latis.ops.GoesImageReaderOperation
import latis.ops.Resample
import latis.ops.RGBImagePivot
import latis.output.ImageWriter
import latis.output.TextWriter

package object dsl {

  implicit class DatasetOps(lhs: Dataset) {

    def info: String = lhs.toString()

    //TODO: redefine high level DSL
    //def resample(min: (Double, Double), max: (Double, Double), count: Int): Dataset = {
    //  val domainSet = BinSet2D.fromExtents(min, max, count)
    //  lhs.withOperation(Resample(domainSet))
    //}

    //def makeRGBImage(r: Double, g: Double, b: Double): Dataset =
    //  lhs.withOperation(RGBImagePivot(r, g, b))

    // This will only work in Jupyter.
    def image: Image = {
      val img = ImageWriter.encode(lhs)
      Image.fromRenderedImage(img, Image.PNG)
    }

    def writeImage(path: String): Unit =
      ImageWriter(path).write(lhs)

    def writeText(out: OutputStream = System.out): Unit =
      output.TextWriter(out).write(lhs)

    def readGoesImages(): Dataset =
      lhs.withOperation(GoesImageReaderOperation())

    def toSpark(): Dataset = lhs.restructureWith(RddFunction)
    def fromSpark(): Dataset = lhs.restructureWith(SeqFunction)
  }
}
