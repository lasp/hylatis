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
import latis.util.StreamUtils._
import latis.util.StreamUtils
import ucar.ma2.Section

class TestGoesAbiReader extends JUnitSuite {

  //@Test
  def bulk_load_goes = {
    val goes = GoesReader().getDataset
    
    val ops: Seq[UnaryOperation] = Seq(
      //Contains("wavelength", 1370.0, 2200.0, 3900.0),
      GeoGridImageResampling(-130, 0, -30, 50, 10000),
      RGBImagePivot(1370.0, 2200.0, 3900.0)
    )
    
    val ds = ops.foldLeft(goes)((ds, op) => op(ds))
    //TextWriter(System.out).write(ds)
    ImageWriter("goesRGB.png").write(ds)
  }

  
//  @Test
//  def read_NetCDF_S3_image = {
//    val ds = GoesImageReader(new URI("s3://goes-001/goes0001.nc")).getDataset()
//    Writer.write(ds)
//  }
  
  
  @Test
  def read_NetCDF_file_image = {
    val s1 = new Section(Array(20,30))
    //val s2 = s1.shiftOrigin(Array(1,1)).shiftOrigin(Array(1,1)) //same size
    //val s0 = new Section(s1.getOrigin, s1.getShape, Array(2,3)) //"shape" is really max index, unless you shift origin
    //val s0a = new Section(s0.getOrigin, s0.getShape, Array(3,2)) //second application of stride in relative to that section, not orig indices
    //val s2 = s1.compose(s0a)
    val s0 = NetcdfFunction.applyStrideToSection(s1, Array(2,3))
    val s2 = NetcdfFunction.applyStrideToSection(s0, Array(3,2))
    println(s1, s1.computeSize())
    println(s0, s0.computeSize())
    println(s2, s2.computeSize())
    
    val s = new Section(Array(0,0), Array(20,30), Array(2,3))
    println(s)
    println(s.getShape.toList)
    println(s.computeSize())
    
//    val ds = GoesImageReader(new URI("file:///data/goes/2018_230_17/OR_ABI-L1b-RadF-M3C16_G16_s20182301700501_e20182301711279_c20182301711333.nc")).getDataset
//    //val s = ds.data.streamSamples.take(5)
//    //StreamUtils.unsafeStreamToSeq(s) foreach println
//    TextWriter(System.out).write(ds)
    
  }
  
  //@Test
  def aws_full_disk_image = {
    //val ds = GoesImageReader(new URI("file://data/goes16/OR_ABI-L1b-RadF-M3C08_G16_s20182301700501_e20182301711267_c20182301711312.nc")).getDataset
    val ds = GoesImageReader(new URI("http://s3.amazonaws.com/noaa-goes16/ABI-L1b-RadF/2018/230/17/OR_ABI-L1b-RadF-M3C08_G16_s20182301700501_e20182301711267_c20182301711312.nc")).getDataset
    //Writer.write(ds)
    ds.data.unsafeForce.samples.head match {
      case Sample(d,r) =>
        println(d)
        println(r)
    }
  }

  //@Test
  def goes_image_files() = {
    val ds = Dataset.fromName("goes_image_files")
    //Writer.write(ds)
    TextWriter(System.out).write(ds)
  }
  
  //@Test
  def calculator = {
    val calc = GOESGeoCalculator("")
    val lat = 10.0
    val lon = -90.0
    var (y, x) = calc.geoToYX((lat, lon)).get
    println(y,x)
  }
  /* lat,lon    y, x
   * 0,0	    (2711.5, 5406)
   * 0,-90    (2711.5, 1893)
   * 40,-115  (822, 1261)
   * -40,-115 (4600, 1261)
  var (y2, x1) = calc.geoToYX((lat1, lon1)).get //TODO: orElse error
  var (y1, x2) = calc.geoToYX((lat2, lon2)).get //TODO: orElse error
   * -10,-110 (3242, 980)  y2, x1
   *  10, -90 (2167, 1908) y1, x2
   */
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