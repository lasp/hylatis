package latis

import java.net.URI

import org.junit._
import org.scalatest.junit.JUnitSuite

import latis.data._
import latis.dataset._
import latis.input._
import latis.ops._
import latis.output._

class TestGoesAbiReader extends JUnitSuite {

  lazy val goesDataset: Dataset = {
    val section = "(500:5000:10, 500:5000:10)" //full: (0:5423, 0:5423)
    import latis.dsl._
    val uri = new URI("file:///data/goes/2018_230_17")
 //TODO use s3 URI
    GoesGranuleListReader
      .read(uri) //wavelength -> uri
      //.stride(4) //limit number of bands
      .select("wavelength < 4000") //first 3 bands
      .toSpark() //use Spark
      .withReader(GoesImageReader(section))    //wavelength -> (y, x) -> radiance
      .uncurry()                               //(wavelength, y, x) -> radiance
      .groupByVariable("x", "y", "wavelength") //(x, y, wavelength) -> radiance
      .cache2()
  }

  //@Test
  def read_netcdf_file(): Unit = {
    val uri = new URI("file:///data/goes/2018_230_17/OR_ABI-L1b-RadF-M3C16_G16_s20182301700501_e20182301711279_c20182301711333.nc")
    val reader = GoesImageReader()
    val ds = reader
      .read(uri)
      .stride(2,2)
    println(ds.unsafeForce().data.sampleSeq.length)
    //TextWriter(System.out).write(ds)
  }

  //@Test
  def goes_image_files(): Unit = {
    val uri = new URI("file:///data/goes/2018_230_17")
    val ds  = GoesGranuleListReader.read(uri) // wavelength -> uri
    TextWriter(System.out).write(ds)
  }

  //@Test
  def rgb_extration(): Unit = {
    val ds = List(1, 2, 3)
    val rs = List(List(1, 2, 3))
    CartesianFunction1D.fromValues(ds, rs).map { cf =>
      println(cf(DomainData(2.1)))
    }
  }

  //@Test
  def geo_grid() = {
    import latis.dsl._
    val g = geoGrid((-114, -43), (-25, 34), 16)
    g.elements.foreach(println)
  }

  @Test
  def dsl_rgb_image() = {
    import latis.dsl._
    goesDataset.makeRGBImage(1370.0, 2200.0, 3900.0)
      .writeImage("/data/goes/goesRGB.png")
  }

  @Test
  def dsl_geo_rgb_image() = {
    import latis.dsl._
    goesDataset
      .geoSubset((-110, -25), (-45, 35), 100000)
      .makeRGBImage(1370.0, 2200.0, 3900.0)
      .writeImage("/data/goes/goesRGB.png")
  }

  @Test
  def geo_rgb_image(): Unit = {
    import latis.dsl._
    import latis.util.GOESUtils._
    val grid = geoGrid((-110, -25), (-45, 35), 100000)

    goesDataset
      .curry(2) //(x, y) -> wavelength -> radiance
      .compose(rgbExtractor(1370.0, 2200.0, 3900.0)) //first 3
      //.compose(rgbExtractor(1370.0, 6900.0, 10300.0)) //match stride of 4
      .substitute(geoCSX) // (lon, lat) -> wavelength -> radiance
      //-114.1, -25.5 to -43.5, 34.8
      .resample(grid)
      .writeImage("/data/goes/goesRGB.png")
    //TODO: image is upside down
  }

  @Test
  def geo_eval(): Unit = {
    import latis.dsl._
    import latis.util.GOESUtils._

    goesDataset
      .curry(2)           //(x, y) -> wavelength -> radiance
      .substitute(geoCSX) // (lon, lat) -> wavelength -> radiance
      .groupByBin(geoGrid((-105, -25), (-45, 25), 100), HeadAggregation())
      .eval(TupleData(-105.0, -25.0))
      .writeText()
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
