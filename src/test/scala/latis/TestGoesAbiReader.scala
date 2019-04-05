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
      GroupBy("ix", "iy")
     , Pivot(Vector(0, 1, 2), Vector("r","g","b")) 
    )
    val image = ops.foldLeft(goes)((ds, op) => op(ds))
    //println("Write final image")
    //Writer.write(image)      // only use for downsampled datasets
    ImageWriter("goesRGB.png").write(image)
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
  def goes_image_files = {
    val ds = Dataset.fromName("goes_image_files")
    Writer.write(ds)
  }
  
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