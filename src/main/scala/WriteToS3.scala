import latis.util.AWSUtils
import latis.input.DatasetSource
import latis.data._
import java.io.File
import java.net.URI


object WriteToS3 extends App {
    val s3 = AWSUtils.s3Client.get
    
//    // Write wavelength file
//    val key = "des_veg_cloud/wavelength.txt"
//    val obj = new File("/data/hysics/des_veg_cloud/wavelength.txt")
//    s3.putObject("hylatis-hysics-001", key, obj)
    
//    val ds = DatasetSource.fromName("hysics_des_veg_cloud_image_files").getDataset()
//    val baseURL = ds("baseURL").getOrElse("")
//    ds.samples foreach {
//      case (_, RangeData(file)) => 
//        val key = file
//        val obj = new File(new URI(s"$baseURL/$file"))
//        println(s"$key  $obj")
//uncomment if you really mean it          s3.putObject("hylatis-hysics-001", key, obj)
//    }
    //Writer().write(ds)
  
}