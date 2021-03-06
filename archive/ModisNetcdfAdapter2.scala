package latis.input

import latis.data._
import latis.util.AWSUtils
import latis.util.LatisConfig
import java.nio.file._
import java.net.URI

import ucar.nc2.NetcdfFile

import latis.ops.Operation

case class ModisNetcdfAdapter2(varName: String, bandIndex: Int) extends Adapter {
  //TODO: AdapterConfig?
  //TODO: get orig varName from the model? metadata?

  def getData(uri: URI, ops: Seq[Operation]): SampledFunction =
    NetcdfFunction2(open(uri), varName, bandIndex)
  
  //TODO: util or inherit from NetcdfAdapter
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
        //TODO: deal with concurrency
        if (! file.exists) AWSUtils.copyS3ObjectToFile(uri, file)
        NetcdfFile.open(file.toString)
      case "file" => 
        NetcdfFile.open(uri.getPath)
      case _    =>
        NetcdfFile.open(uri.getScheme + "://" + uri.getHost + "/" + uri.getPath)
    }
  }
}


import scala.collection.JavaConverters._
import latis.util.StreamUtils
import ucar.ma2.{Range => NRange,Section}
import latis.util.LatisConfig
import latis.util.AWSUtils
import latis.util.AWSUtils
import java.io.File
import java.nio.file.Files
import java.nio.file.Files
import latis.util.LatisConfig
import latis.util.LatisConfig

/**
 * Express a NetCDF file as a SampledFunction.
 */
case class NetcdfFunction2(ncFile: NetcdfFile, varName: String, bandIndex: Int) extends MemoizedFunction {
  //TODO: factor out class?
  //TODO: NetcdfDataset?
  //TODO: override "force" to make ArrayFunctionND
  //TODO: use model instead of single varName
  
  def sampleSeq: Seq[Sample] = {
    //Assume 3D array, for now
    // (band, along-track, along-scan) -> f
    // (w, x, y) -> f
    // Note that we should change the Hysics model
    // MODIS docs have the cross-track direction "backwards" such that our x,y is right-handed with z up
    // Unidata CDM uses (track, xtrack), which is consistent with our (x, y) but doesn't clarify xtrack direction
    // Another source defines the other direction
    //???which does Hysics use???
    // Should we use standard names for swath dimensions instead of x, y?
    
    val ncvar = ncFile.findVariable(varName)
    
    //val bands: Array[Double] = getBands(varName)
    
    val scales:  Array[Float] = ncvar.findAttribute("radiance_scales")
      .getValues.copyTo1DJavaArray.asInstanceOf[Array[Float]]
    val offsets: Array[Float] = ncvar.findAttribute("radiance_offsets")
      .getValues.copyTo1DJavaArray.asInstanceOf[Array[Float]]
    
    val shape = ncvar.getShape //e.g. [15, 2030, 1354]
    val stride = LatisConfig.getOrElse("hylatis.modis.stride", 1)
    val section = new Section(
      new NRange(bandIndex, bandIndex),
      new NRange(0, shape(1)-1, stride),
      new NRange(0, shape(2)-1, stride)
    )
    val (nw, nx, ny) = section.getShape match {case Array(nw, nx, ny) => (nw, nx, ny)}
    
    val ncarr = ncvar.read(section)
    val samples = for {
      //iw <- 0 until nw
      ix <- 0 until nx
      iy <- 0 until ny
      index = iy + ix * ny
      value = ncarr.getShort(index) match {
        // Invalid if raw scaled int > 32767
        //TODO: add support for valid min and valid max
        case si if si > 32767 => Float.NaN
        case si => scales(bandIndex) * (si - offsets(bandIndex))
      }
    } yield Sample(DomainData(ix*stride, iy*stride), RangeData(value))
    
    //StreamUtils.seqToIOStream(samples)
    samples
  }
  
//  /**
//   * Get the values of the variable representing the band/wavelength dimension.
//   * Each 3D radiance variable in a MODIS 021KM file has a corresponding band variable.
//   */
//  //def getBands(varName: String): Array[Float] = {
//  // Make these Doubles so eval will work
//  def getBands(varName: String): Array[Double] = {
//    //TODO: define a Dataset: i -> band then substitute
//    val bandName = varName match {
//      case s if s endsWith "EV_1KM_RefSB" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_1KM_RefSB"
//      case s if s endsWith "EV_1KM_Emissive" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_1KM_Emissive"
//      case s if s endsWith "EV_250_Aggr1km_RefSB" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_250M"
//      case s if s endsWith "EV_500_Aggr1km_RefSB" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_500M"
//    }
//    ncFile.findVariable(bandName).read.copyTo1DJavaArray.asInstanceOf[Array[Float]].map(_.toDouble)
//  }
  
//  /**
//   * Consider this SampledFunction empty if all the dimensions
//   * in the NetCDF file have zero length. Presumably, an empty
//   * NetCDF file would return an empty list of Dimensions.
//   */
//  def isEmpty: Boolean =
//    ncFile.getDimensions.asScala.forall(_.getLength == 0)

}
