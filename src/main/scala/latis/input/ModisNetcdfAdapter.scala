package latis.input

import latis.data._

import cats.effect.IO
import fs2.Stream
import java.net.URI
import ucar.nc2.NetcdfFile

case class ModisNetcdfAdapter(varName: String) extends Adapter {
  //TODO: AdapterConfig?
  //TODO: get orig varName from the model? metadata?
  
  def apply(uri: URI): SampledFunction =
    NetcdfFunction(open(uri), varName)
  
  /**
    * Return a NetcdfFile
    */
    //TODO: move to generic NetCDF adapter
  def open(uri: URI): NetcdfFile = {
    uri.getScheme match {
      case null => 
        NetcdfFile.open(uri.getPath) //assume file path
      case "s3" => 
        val uriExpression = uri.getScheme + "://" + uri.getHost + uri.getPath
        val raf = new ucar.unidata.io.s3.S3RandomAccessFile(uriExpression, 1<<15, 1<<24)
        NetcdfFile.open(raf, uriExpression, null, null)
      //TODO:  "file"
      case _    =>
        NetcdfFile.open(uri.getScheme + "://" + uri.getHost + "/" + uri.getPath)
    }
  }
}


import scala.collection.JavaConverters._
import latis.util.StreamUtils

/**
 * Express a NetCDF file as a SampledFunction.
 */
case class NetcdfFunction(ncFile: NetcdfFile, varName: String) extends SampledFunction {
  //TODO: factor out class?
  //TODO: NetcdfDataset?
  //TODO: override "force" to make ArrayFunctionND
  
  def streamSamples: Stream[IO, Sample] = {
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
    
    val bands: Array[Float] = getBands(varName)
    val nw = bands.length
    
    val scales:  Array[Float] = ncvar.findAttribute("radiance_scales")
      .getValues.copyTo1DJavaArray.asInstanceOf[Array[Float]]
    val offsets: Array[Float] = ncvar.findAttribute("radiance_offsets")
      .getValues.copyTo1DJavaArray.asInstanceOf[Array[Float]]
    
    val shape = ncvar.getShape //15, 2030, 1354
    //val (nx, ny) = (shape(1), shape(2))
val (nx, ny) = (20,20) //TODO: apply scale factor
    val ncarr = ncvar.read(Array(0,0,0), Array(nw,nx,ny)) //read into memory yet? slow so probably yes
    val samples = for {
      iw <- 0 until nw
      ix <- 0 until nx
      iy <- 0 until ny
      index = iy + ix * ny + iw * nx * ny
      value = scales(iw) * (ncarr.getShort(index) - offsets(iw))
    } yield Sample(DomainData(bands(iw),ix,iy), RangeData(value))
    
    StreamUtils.seqToIOStream(samples)
  }
  
  /**
   * Get the values of the variable representing the band/wavelength dimension.
   * Each 3D radiance variable in a MODIS 021KM file has a corresponding band variable.
   */
  def getBands(varName: String): Array[Float] = {
    val bandName = varName match {
      case s if s endsWith "EV_1KM_RefSB" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_1KM_RefSB"
      case s if s endsWith "EV_1KM_Emissive" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_1KM_Emissive"
      case s if s endsWith "EV_250_Aggr1km_RefSB" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_250M"
      case s if s endsWith "EV_500_Aggr1km_RefSB" => "MODIS_SWATH_Type_L1B/Data_Fields/Band_500M"
    }
    ncFile.findVariable(bandName).read.copyTo1DJavaArray.asInstanceOf[Array[Float]]
  }
  
  /**
   * Consider this SampledFunction empty if all the dimensions
   * in the NetCDF file have zero length. Presumably, an empty
   * NetCDF file would return an empty list of Dimensions.
   */
  def isEmpty: Boolean =
    ncFile.getDimensions.asScala.forall(_.getLength == 0)

}
