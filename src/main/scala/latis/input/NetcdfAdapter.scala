package latis.input

import latis.data._
import latis.model._

import java.net.URI
import ucar.nc2.NetcdfFile
import latis.util.AWSUtils
import java.nio.file._

case class NetcdfAdapter(model: DataType) extends Adapter {
  //TODO: AdapterConfig?
  //TODO: consider using ucar.nc2.NetcdfDataset, enhanced
  
  def apply(uri: URI): SampledFunction =
    NetcdfFunction(open(uri), model)
  
  /**
    * Return a NetcdfFile for the given URI.
    */
  def open(uri: URI): NetcdfFile = {
    uri.getScheme match {
      case null => 
        NetcdfFile.open(uri.getPath) //assume file path
      case "s3" => 
        //TODO: utils
        val (bucket, key) = AWSUtils.parseS3URI(uri)
        val s3is = AWSUtils.s3Client.get.getObject(bucket, key).getObjectContent
        val tmpDir = Files.createTempDirectory("latis").toString
        val path = FileSystems.getDefault().getPath(tmpDir, key)
        Files.copy(s3is, path)
        s3is.close
        NetcdfFile.open(path.toString)
//        val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
//        val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
//        NetcdfFile.open(raf, uriExpression, null, null)
      //TODO:  "file"
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

