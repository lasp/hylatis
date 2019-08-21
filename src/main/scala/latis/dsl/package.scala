package latis

import almond.display.Image

import latis.data.BinSet2D
import latis.model.Dataset
import latis.ops.Resample
import latis.ops.RGBImagePivot
import latis.output.ImageWriter

package object dsl {

  implicit class DatasetOps(lhs: Dataset) {

    def info: String = lhs.toString()

    def resample(min: (Double, Double), max: (Double, Double), count: Int): Dataset = {
      val domainSet = BinSet2D.fromExtents(min, max, count)
      Resample(domainSet)(lhs)
    }

    def makeRGBImage(r: Double, g: Double, b: Double): Dataset =
      RGBImagePivot(r, g, b)(lhs)

    // This will only work in Jupyter.
    def image: Image = {
      val img = ImageWriter.encode(lhs)
      Image.fromRenderedImage(img, Image.PNG)
    }

    def writeImage(path: String): Unit =
      ImageWriter(path).write(lhs)
  }
}
