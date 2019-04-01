package latis

import org.junit._
import org.junit.Assert._
import latis.util.AWSUtils

class TestAWS {
  
  //@Test
  def list_bucket = {
    import scala.collection.JavaConversions._
    val s3 = AWSUtils.s3Client.get
    val os = s3.listObjects("noaa-goes16").getObjectSummaries
    //val os = s3.listObjects("noaa-goes16", "ABI-L1b-RadC/").getObjectSummaries
    //val os = s3.listObjects("hylatis-hysics-001", "des_veg_cloud/").getObjectSummaries
    println(os.length)
    os.foreach(o => println(o.getKey))
  }
}