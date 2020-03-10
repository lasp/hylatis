package latis

import java.io.OutputStream

import almond.display.Image
import org.apache.spark.storage.StorageLevel

import latis.data._
import latis.dataset.ComputationalDataset
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.ops.GoesImageReaderOperation
import latis.ops.GroupByBin
import latis.ops.HeadAggregation
import latis.output.ImageWriter
import latis.util.GOESUtils
import latis.util.HysicsUtils
import latis.util.LatisException
import latis.util.ModisUtils

package object dsl {

  implicit class DatasetOps(lhs: Dataset) {

    def info: String = lhs.toString()

    //Define high level DSL, curry if needed
    def geoSubset(min: (Double, Double), max: (Double, Double), count: Int): Dataset = {
      // Curry 2 if not already
      val ds2D =
        if (lhs.model.arity > 2) lhs.curry(2)
        else lhs
      // Transform to geo if not already
      val ds = ds2D.model match {
        case Function(Tuple(s, _), _) =>
          if (s.id.toLowerCase.take(3) == "lon") ds2D
          // Do CSX if not already on lon-lat grid
          else lhs.id match {
            case s if s.startsWith("goes")   => ds2D.substitute(GOESUtils.geoCSX)
            case s if s.startsWith("hysics") => ds2D.substitute(HysicsUtils.geoCSX)
            case s if s.startsWith("modis")  => ds2D.substitute(ModisUtils.geoCSX)
          }
      }
      val grid = geoGrid(min, max, count)
      ds.resample(grid)
    }

    def makeRGBImage(r: Double, g: Double, b: Double): Dataset = {
      val ds =
        if (lhs.model.arity > 2) lhs.curry(2)
        else lhs
      ds.compose(rgbExtractor(r, g, b))
    }


    // This will only work in Jupyter.
    //TODO: showImage?
    def image: Image = {
      val img = ImageWriter.encode(lhs)
      Image.fromRenderedImage(img, Image.PNG)
    }

    //TODO: add writeGeoTIFF

    def writeImage(path: String): Unit =
      ImageWriter(path).write(lhs)

    def writeText(out: OutputStream = System.out): Unit =
      output.TextWriter(out).write(lhs)

    def readGoesImages(): Dataset =
      lhs.withOperation(GoesImageReaderOperation())

    def toSpark(): Dataset = lhs.restructureWith(RddFunction)
    def fromSpark(): Dataset = lhs.restructureWith(SeqFunction)

    def resample(domainSet: DomainSet): Dataset =
      lhs.withOperation(GroupByBin(domainSet, HeadAggregation()))

    def cache2(): Dataset = {
      val ds = lhs.unsafeForce() match {
        case mds: MemoizedDataset => mds.data match {
          case RddFunction(rdd) =>
            val newRDD = rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
            newRDD.count
            new MemoizedDataset(
              mds.metadata,
              mds.model,
              RddFunction(newRDD)
            )
          case _ => mds //No RDD to cache
        }
      }
      ds.cache()
      ds
    }

  }

  def geoGrid(min: (Double, Double), max: (Double, Double), count: Int): DomainSet = {
    val model = Tuple(
      Scalar(Metadata("lon") + ("type" -> "double")),
      Scalar(Metadata("lat") + ("type" -> "double"))
    )
    BinSet2D.fromExtents(min, max, count, model)
    //val n = Math.round(Math.sqrt(count)).toInt //TODO: preserve aspect ratio
    //val x0 = min._1
    //val y0 = min._2
    //val dx = (max._1 - min._1) / n
    //val dy = (max._2 - min._2) / n
    ////-130, 0, -30, 50   //-114.1, -25.5 to -43.5, 34.8
    ////val xset = BinSet1D(-110, 0.65, 100)
    ////val yset = BinSet1D(-25, 0.55, 100)
    //val xset = BinSet1D(x0, dx, n)
    //val yset = BinSet1D(y0, dy, n)
    //new BinSet2D(xset, yset, model)
  }


  def rgbExtractor(wr: Double, wg: Double, wb: Double): ComputationalDataset = {
    val model = Function(
      Function(
        Scalar(Metadata("wavelength") + ("type" -> "double")),
        Scalar(Metadata("radiance") + ("type" -> "double"))
      ),
      Tuple(
        Scalar(Metadata("r") + ("type" -> "double")),
        Scalar(Metadata("g") + ("type" -> "double")),
        Scalar(Metadata("b") + ("type" -> "double"))
      )
    )
    val md = Metadata("rgbExtractor")
    val f = extractRGB(wr, wg, wb)
    ComputationalDataset(md, model, f)
  }

  // (w -> f) => (r, g, b)
  def extractRGB(wr: Double, wg: Double, wb: Double): Data => Either[LatisException, Data] =
    (data: Data) => data match {
      case spectrum: MemoizedFunction => spectrum
        // Uses default SeqFunction without searching (which requires ordering)
        val color = for {
          r <- spectrum(DomainData(wr))
          g <- spectrum(DomainData(wg))
          b <- spectrum(DomainData(wb))
        } yield TupleData(r ++ g ++ b)

        if (color.isRight) color
        else Right(TupleData(Double.NaN, Double.NaN, Double.NaN))
        //TODO: no longer need either
      case _ => ??? //TODO: invalid arg, required spectrum
    }
}
