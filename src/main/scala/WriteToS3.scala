import latis.util.AWSUtils
import latis.reader.DatasetSource
import latis.data.Sample
import latis.model.Text
import java.io.File
import java.net.URI


object WriteToS3 extends App {
    val s3 = AWSUtils.s3Client.get
    
//    // Write wavelength file
//    val key = "des_veg_cloud/wavelength.txt"
//    val obj = new File("/data/hysics/des_veg_cloud/wavelength.txt")
//    s3.putObject("hylatis-hysics-001", key, obj)
    
    val ds = DatasetSource.fromName("hysics_des_veg_cloud_image_files").getDataset()
    val baseURL = ds.getProperty("baseURL", "")
    ds foreach {
      case Sample(_, d) => d match { //TODO: can't do nested match on Text here
        case Text(file) => 
          val key = file
          val obj = new File(new URI(s"$baseURL/$file"))
          println(s"$key  $obj")
//uncomment if you really mean it          s3.putObject("hylatis-hysics-001", key, obj)
      }
    }
    //Writer().write(ds)
  
}