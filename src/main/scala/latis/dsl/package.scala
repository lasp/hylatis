package latis

import java.io.OutputStream

import almond.display.Image
import org.apache.spark.storage.StorageLevel

import latis.data._
import latis.dataset.Dataset
import latis.dataset.DatasetFunction
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.ops.GoesImageReaderOperation
import latis.output.ImageWriter
import latis.util.LatisException

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

    def cache(): Dataset = {
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
    val n = Math.round(Math.sqrt(count)).toInt //TODO: preserve aspect ratio
    val x0 = min._1
    val y0 = min._2
    val dx = (max._1 - min._1) / n
    val dy = (max._2 - min._2) / n
    //-130, 0, -30, 50   //-114.1, -25.5 to -43.5, 34.8
    //val xset = BinSet1D(-110, 0.65, 100)
    //val yset = BinSet1D(-25, 0.55, 100)
    val xset = BinSet1D(x0, dx, n)
    val yset = BinSet1D(y0, dy, n)
    new BinSet2D(xset, yset, model)
    //TODO: allow setting model of BinSet2D instead of (_1,_2)
  }


  def rgbExtractor(wr: Double, wg: Double, wb: Double): DatasetFunction = {
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
    DatasetFunction(md, model, f)
  }

  // (w -> f) => (r, g, b)
  def extractRGB(wr: Double, wg: Double, wb: Double): Data => Either[LatisException, Data] =
    (data: Data) => data match {
      case spectrum: MemoizedFunction => spectrum
        // Use fill value is incoming spectrum is empty
        if (spectrum.sampleSeq.isEmpty) Right(TupleData(Double.NaN, Double.NaN, Double.NaN))
        else {
          // Put data into CartesianFunction1D TODO: with NN interp
          //TODO: build CartF from samples via FF
          val ds: IndexedSeq[Datum] = spectrum.sampleSeq.toVector.map {
            case Sample(DomainData(d: Datum), _) => d
          } //.reverse
          val rs: IndexedSeq[Data] = spectrum.sampleSeq.toVector.map {
            case Sample(_, r) => Data.fromSeq(r)
          } //.reverse

          CartesianFunction1D.fromData(ds, rs).flatMap { f =>
            for {
              r <- f(DomainData(wr))
              g <- f(DomainData(wg))
              b <- f(DomainData(wb))
            } yield TupleData(r ++ g ++ b)
          }
        }
      case _ => ??? //TODO: invalid arg, required spectrum
    }
}
