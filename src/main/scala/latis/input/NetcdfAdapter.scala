package latis.input

import latis.data._
import latis.model._

import java.net.URI
import ucar.nc2.NetcdfFile
import latis.util.AWSUtils
import java.nio.file._
import latis.util.LatisConfig
import latis.util.ConfigLike
import ucar.nc2.dataset.NetcdfDataset
import ucar.ma2.Section

case class NetcdfAdapter(
  model: DataType, 
  config: NetcdfAdapter.Config = NetcdfAdapter.Config()
) extends Adapter {
  
  def apply(uri: URI): SampledFunction =
    NetcdfFunction(open(uri), model, config.section)
  
  /**
   * Return a NetcdfFile from the given URI.
   */
  def open(uri: URI): NetcdfDataset = {
    //TODO: resource management, make sure this gets closed
    uri.getScheme match {
      case null => 
        NetcdfDataset.openDataset(uri.getPath) //assume file path
      case "s3" => 
        // Create a local file name
        val (bucket, key) = AWSUtils.parseS3URI(uri)
        val dir = LatisConfig.get("file.cache.dir") match {
          case Some(dir) => dir
          case None => Files.createTempDirectory("latis").toString
        }
        val file = Paths.get(dir, bucket, key).toFile
        // If the file does not exist, make a local copy
        //TODO: deal with concurrency
        if (! file.exists) AWSUtils.copyS3ObjectToFile(uri, file)
        NetcdfDataset.openDataset(file.toString)
      case "file" => 
        NetcdfDataset.openDataset(uri.getPath)
      case _    =>
        NetcdfDataset.openDataset(uri.getScheme + "://" + uri.getHost + "/" + uri.getPath)
    }
  }
}


object NetcdfAdapter {
  
  case class Config(properties: (String, String)*) extends ConfigLike {
    val section: Option[Section] = get("section").map(new Section(_))
  }
}
