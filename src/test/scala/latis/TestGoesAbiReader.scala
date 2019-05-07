package latis

import io.findify.s3mock._
import org.junit._
import org.junit.Assert._
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

class TestGoesAbiReader {

  //@Test
  def bulk_load_goes = {
    val reader = GoesReader()
    val goes = reader.getDataset
    //println("dataSet: " + goes)
    //println("  model: " + goes.model)
    //println("  metadata: " + goes.metadata.properties)
    //println("  data: " + goes.data)
    //Writer.write(goes)
    val ops: Seq[UnaryOperation] = Seq(
      //GeoBoundingBox(-110, 30, -100, 40),  //Breaks index logic later
      //Contains("wavelength", 1370.0, 2200.0, 3900.0),
      GroupBy("ix", "iy"),
      //GroupBy("iy", "ix"),
      Pivot(Vector(1370.0, 2200.0, 3900.0), Vector("r","g","b")) 
    )
    val image = ops.foldLeft(goes)((ds, op) => op(ds))

    val ds = image.restructure(GoesArrayFunction2D) //TODO: do in GoesGridEvaluation
    
    //val gridOp = GoesGridEvaluation(-135.0, 50.0, -65.0, 25.0, 100)
    val gridOp = GoesGridEvaluation(-130.0, 20.0, -65.0, 55.0, 1000000)
    //val gridOp = GoesGridEvaluation(-110, -10, -90, 10, 1000000)
    val ds2 = gridOp(ds) 
    //TODO: not ordered
    
    //TextWriter(System.out).write(ds2)
    ImageWriter("goesRGB.png").write(ds2)
  }
  
  //@Test
  def replicate_service = {
    val goes = GoesReader().getDataset
    val ops = Seq(
      RGBImagePivot("wavelength", 1370.0, 2200.0, 3900.0),
      GoesGridEvaluation(-135.0, 25.0, -65.0, 50.0, 100)
    )
    val ds = ops.foldLeft(goes)((ds, op) => op(ds))
    
    TextWriter(System.out).write(ds)
    //ImageWriter("goesRGB.png").write(ds)
  }
  
//  @Test
//  def read_NetCDF_S3_image = {
//    val ds = GoesImageReader(new URI("s3://goes-001/goes0001.nc")).getDataset()
//    Writer.write(ds)
//  }
  
  
//  @Test
//  def read_NetCDF_file_image = {
//    val ds = GoesImageReader(new URI("file://data/s3/goes-001/goes0001.nc")).getDataset()
//    Writer.write(ds)
//  }
  
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