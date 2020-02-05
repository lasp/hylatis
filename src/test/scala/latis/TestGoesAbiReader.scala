package latis

import io.findify.s3mock._
import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import scala.io.Source

import latis.input._
import latis.output._
import latis.util.AWSUtils
import latis.util.SparkUtils
import latis.metadata._
import latis.model._
import latis.data._
import latis.ops._
import java.net.URL
import java.net.URI
import java.io.File
import java.awt.Color

import latis.ops.Operation
import latis.ops.Uncurry
import cats.effect.IO

import latis.util.GOESUtils.GOESGeoCalculator
import fs2._
import os./

import latis.util.StreamUtils._
import latis.util.StreamUtils
import ucar.ma2.Section

import latis.dataset.DatasetFunction
import latis.util.GOESUtils
import latis.util.HysicsUtils
import latis.util.LatisException

class TestGoesAbiReader extends JUnitSuite {

//  //@Test
//  def bulk_load_goes = {
//    val goes = GoesReader().getDataset
//
//    val ops: Seq[UnaryOperation] = Seq(
//      //Contains("wavelength", 1370.0, 2200.0, 3900.0),
//      GeoGridImageResampling(-130, 0, -30, 50, 10000),
//      RGBImagePivot(1370.0, 2200.0, 3900.0)
//    )
//
//    val ds = ops.foldLeft(goes)((ds, op) => op(ds))
//    //TextWriter(System.out).write(ds)
//    ImageWriter("goesRGB.png").write(ds)
//  }
//
//
////  @Test
////  def read_NetCDF_S3_image = {
////    val ds = GoesImageReader(new URI("s3://goes-001/goes0001.nc")).getDataset()
////    Writer.write(ds)
////  }


  @Test
  def read_netcdf_file = {
    val stride = Array(2712, 2712)
    val uri = new URI("file:///data/goes/2018_230_17/OR_ABI-L1b-RadF-M3C16_G16_s20182301700501_e20182301711279_c20182301711333.nc")
    val reader = GoesImageReader
    val ds = reader.read(uri)
    //  .withOperation(Stride(stride))
    TextWriter(System.out).write(ds)
  }

  @Test
  def goes_image_files() = {
    val uri = new URI("file:///data/goes/2018_230_17")
    val ds = GoesGranuleListReader.read(uri)

    TextWriter(System.out).write(ds)
  }

  val geoCSX: DatasetFunction = {
    val md = Metadata("goes_geo_csx")
    val model = Function(
      Tuple(
        Scalar(Metadata("x") + ("type" -> "double")),
        Scalar(Metadata("y") + ("type" -> "double"))
      ),
      Tuple(
        Scalar(Metadata("lon") + ("type" -> "double")),
        Scalar(Metadata("lat") + ("type" -> "double"))
      )
    )

    val csx: ((Double, Double)) => (Double, Double) =
      GOESUtils.xyToGeo

    val f: TupleData => Either[LatisException, TupleData] = {
      (td: TupleData) => td.elements match {
        case List(Number(x), Number(y)) =>
          csx((x, y)) match {
            case (lon, lat) =>
              //if (lon.isNaN || lat.isNaN) {
              //  Left(LatisException("Invalid coordinate"))
              //} else {
                val dd = DomainData(lon, lat)
                Right(TupleData(dd))
              //}
          }
      }
    }

    DatasetFunction(md, model, f)
  }

  // (w -> f) => (r, g, b)
  def extractRGB(wr: Double, wg: Double, wb: Double): TupleData => Either[LatisException, TupleData] =
    (td: TupleData) => {
      val spectrum: MemoizedFunction = td.elements.head match {
        //TODO: NN interp
        //TODO: use fill value if empty
        case mf: MemoizedFunction => mf
      }
      // Use fill value is incoming spectrum is empty
      if (spectrum.sampleSeq.isEmpty) Right(TupleData(Double.NaN, Double.NaN, Double.NaN))
      else {
        // Put data into IndexedFunction1D with NN interp
        val ds: Seq[Datum] = spectrum.sampleSeq.map {
          case Sample(DomainData(d: Datum), _) => d
        }
        val rs: Seq[TupleData] = spectrum.sampleSeq.map {
          case Sample(_, r) => TupleData(r)
        }
        val f = IndexedFunction1D(ds, rs)

        for {
          r <- f(TupleData(DomainData(wr)))
          g <- f(TupleData(DomainData(wg)))
          b <- f(TupleData(DomainData(wb)))
        } yield TupleData(r.elements ++ g.elements ++ b.elements)
      }
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

  @Test
  def rgb_extration(): Unit = {
    val ds = List(Data.DoubleValue(1), Data.DoubleValue(2), Data.DoubleValue(3))
    val rs = List(Data.DoubleValue(1), Data.DoubleValue(2), Data.DoubleValue(3)).map(TupleData(_))
    val spectrum = IndexedFunction1D(ds, rs)
    println(spectrum(TupleData(Data.DoubleValue(2.1))))
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

  @Test
  def geo_grid() = {
    val g = geoGrid((-114, -43), (-25, 34), 16)
    g.elements.foreach(println)
  }

  @Test
  def goes_cube(): Unit = {
    import latis.dsl._
    val uri = new URI("file:///data/goes/2018_230_17")
    val ds = GoesGranuleListReader.read(uri) //wavelength -> uri
      .stride(4)  //wavelengths: 1370, 6900, 10300
      .toSpark() //.restructureWith(RddFunction)  //use Spark
      //.withOperation(ReaderOperation(GoesImageReader)) //wavelength -> (y, x) -> radiance
        //spark serialization error in ReaderOperation: GoesImageReader
      .readGoesImages() //wavelength -> (y, x) -> radiance
      .uncurry() //(wavelength, y, x) -> radiance
      .groupByVariable("x", "y", "wavelength") //(x, y, wavelength) -> radiance
      // canonical cube
      .curry(2) //(x, y) -> wavelength -> radiance
      .compose(rgbExtractor(1370.0, 6900.0, 10300.0))
      .substitute(geoCSX) // (lon, lat) -> wavelength -> radiance
        //yikes, NaN domain values for off disk,
        //TODO: GBB.compose(sub), avoid non-cartesian grid (can't plot)
      //.select("lon >= -114")
      //.select("lon < -25")
      //.select("lat >= -43")
      //.select("lat < 34")
      //.fromSpark() //since GBB leaves gaps in spark
      //-114.1, -25.5 to -43.5, 34.8
      //.groupByBin(geoGrid((-114, -43), (-25, 34), 100000), HeadAggregation())
      .groupByBin(geoGrid((-105, -25), (-45, 25), 100000), HeadAggregation())
      //.writeText()
      .writeImage("/data/goes/goesRGB.png")
      //println(ds.unsafeForce().data.sampleSeq.length)
  }

//  //@Test
//  def calculator = {
//    val calc = GOESGeoCalculator("")
//    val lat = 10.0
//    val lon = -90.0
//    var (y, x) = calc.geoToYX((lat, lon)).get
//    println(y,x)
//  }
//  /* lat,lon    y, x
//   * 0,0	    (2711.5, 5406)
//   * 0,-90    (2711.5, 1893)
//   * 40,-115  (822, 1261)
//   * -40,-115 (4600, 1261)
//  var (y2, x1) = calc.geoToYX((lat1, lon1)).get //TODO: orElse error
//  var (y1, x2) = calc.geoToYX((lat2, lon2)).get //TODO: orElse error
//   * -10,-110 (3242, 980)  y2, x1
//   *  10, -90 (2167, 1908) y1, x2
//   */
}

 


//object TestGoes {
//  
//  private val s3mock: S3Mock = S3Mock(port = 8001, dir = "/data/s3")
//  
//  @BeforeClass
//  def startS3Mock: Unit = s3mock.start
//  
//  @AfterClass
//  def stopS3Mock: Unit = s3mock.stop
//}
