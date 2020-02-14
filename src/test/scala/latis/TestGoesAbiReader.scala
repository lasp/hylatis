package latis

import io.findify.s3mock._
import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import scala.io.Source

import latis.dataset._
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
import latis.util.LatisConfig
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
    val stride = Array(2, 2)
    val uri = new URI("file:///data/goes/2018_230_17/OR_ABI-L1b-RadF-M3C16_G16_s20182301700501_e20182301711279_c20182301711333.nc")
    val reader = GoesImageReader
    val ds = reader.read(uri)
      .stride(stride)
    println(ds.unsafeForce().data.sampleSeq.length)
    //TextWriter(System.out).write(ds)
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

  @Test
  def rgb_extration(): Unit = {
    val ds = List(Data.DoubleValue(1), Data.DoubleValue(2), Data.DoubleValue(3))
    val rs = List(Data.DoubleValue(1), Data.DoubleValue(2), Data.DoubleValue(3)).map(TupleData(_))
    val spectrum = IndexedFunction1D(ds, rs)
    println(spectrum(TupleData(Data.DoubleValue(2.1))))
  }

  @Test
  def geo_grid() = {
    import latis.dsl._
    val g = geoGrid((-114, -43), (-25, 34), 16)
    g.elements.foreach(println)
  }

  @Test
  def goes_cube(): Unit = {
    import latis.dsl._
    val uri = new URI("file:///data/goes/2018_230_17")
    GoesGranuleListReader.read(uri) //wavelength -> uri
      .stride(LatisConfig.getOrElse("hylatis.goes.stride", 1))
      .toSpark() //.restructureWith(RddFunction)  //use Spark
      //.withOperation(ReaderOperation(GoesImageReader)) //wavelength -> (y, x) -> radiance
        //spark serialization error in ReaderOperation: GoesImageReader
      .readGoesImages() //wavelength -> (y, x) -> radiance
      .uncurry() //(wavelength, y, x) -> radiance
      .groupByVariable("x", "y", "wavelength") //(x, y, wavelength) -> radiance
      // canonical cube
      .cacheRDD()

    val ds = Dataset.fromName("goes")
      .curry(2) //(x, y) -> wavelength -> radiance
      .compose(rgbExtractor(1370.0, 6900.0, 10300.0)) //match stride of 4
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
      .groupByBin(geoGrid((-105, -25), (-45, 25), 100), HeadAggregation())
      .writeImage("/data/goes/goesRGB.png")
      //println(ds.unsafeForce().data.sampleSeq.length)

      //.writeText()
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
