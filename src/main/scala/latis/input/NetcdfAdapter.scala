package latis.input

import latis.data._
import latis.model._

import java.net.URI
import ucar.nc2.NetcdfFile
import latis.util.AWSUtils
import java.nio.file._
import latis.util.LatisConfig

case class NetcdfAdapter(model: DataType) extends Adapter {
  //TODO: AdapterConfig?
  //TODO: consider using ucar.nc2.NetcdfDataset, enhanced
  
  def apply(uri: URI): SampledFunction =
    NetcdfFunction(open(uri), model)
  
  /**
   * Return a NetcdfFile from the given URI.
   */
  def open(uri: URI): NetcdfFile = {
    //TODO: resource management, make sure this gets closed
    uri.getScheme match {
      case null => 
        NetcdfFile.open(uri.getPath) //assume file path
      case "s3" => 
        // Create a local file name
        val (bucket, key) = AWSUtils.parseS3URI(uri)
        val dir = LatisConfig.get("file.cache.dir") match {
          case Some(dir) => dir
          case None => Files.createTempDirectory("latis").toString
        }
        val file = Paths.get(dir, bucket, key).toFile
        // If the file does not exist, make a local copy
        if (! file.exists) AWSUtils.copyS3ObjectToFile(uri, file)
        NetcdfFile.open(file.toString)
      case "file" => 
        NetcdfFile.open(uri.getPath)
      case _    =>
        NetcdfFile.open(uri.getScheme + "://" + uri.getHost + "/" + uri.getPath)
    }
  }
}

/*
 * TODO: use NetcdfAdapter.Config
 *   scale_attribute = scale_factor
 *   offset_attribute = add_offset
 *   scale = 1
 *   offset = 0
 * which would win?
 * but could be diff for each variable
 * generally need support for variable level properties
 *   Map id => value?
 */

