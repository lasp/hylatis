package latis.input

import java.net.URI
import latis.data._
import latis.model._
import ucar.nc2.NetcdfFile
import latis.model.DataType
import fs2.Stream
import cats.effect.IO

case class ModisNetcdfAdapter(model: DataType) extends Adapter {
  //TODO: AdapterConfig?
  
  def apply(uri: URI): SampledFunction = {
    NetcdfFunction(model, open(uri))
  }  
  
  /**
    * Return a NetcdfFile
    */
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
case class NetcdfFunction(model: DataType, ncFile: NetcdfFile) extends SampledFunction {
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
    
    val id = model match {
      case Function(_, s: Scalar) => s.id
      case _ => ??? //TODO: error
    }
    val groupName = "MODIS_SWATH_Type_L1B/Data_Fields"
    val name = s"${groupName}/$id"
    
    val ncvar = ncFile.findVariable(name)
    val shape = ncvar.getShape //15, 2030, 1354
    //val (nw, nx, ny) = (shape(0), shape(1), shape(2))
    val (nw, nx, ny) = (3,500,500)
    val ncarr = ncvar.read(Array(0,0,0), Array(nw,nx,ny)) //read into memory yet? slow so probably yes
    val samples = for {
      iw <- 0 until nw
      ix <- 0 until nx
      iy <- 0 until ny
      index = iy + ix * ny + iw * nx * ny
      value = ncarr.getShort(index)
    } yield Sample(DomainData(iw,ix,iy), RangeData(value))
    
    StreamUtils.seqToIOStream(samples)
  }
  
  /**
   * Consider this SampledFunction empty if all the dimensions
   * in the NetCDF file have zero length.
   */
  def isEmpty: Boolean =
    ncFile.getDimensions.asScala.forall(_.getLength == 0)

}
