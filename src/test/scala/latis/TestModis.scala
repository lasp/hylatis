package latis

import latis.data._
import latis.input.ModisReader
import latis.model._
import latis.ops.RGBImagePivot
import latis.output.ImageWriter

import org.junit._
import scala.collection.mutable.SortedMap
import latis.util.CoordinateSystemTransform
import latis.metadata.Metadata
import latis.output.TextWriter
import scala.collection.mutable.Buffer
import latis.util.StreamUtils
import cats.effect.IO
import latis.data.SetFunction
import latis.resample.BinResampling
import latis.data.LinearSet2D
import java.io.FileOutputStream
import latis.input.ModisGeolocationReader

class TestModis {
  
  //@Test
  def reader() = {
    //Note: we can override the config:
    //System.setProperty("hylatis.modis.uri", "/data/modis/MYD021KM.A2014230.2120.061.2018054180853.hdf")
    val ds = ModisReader().getDataset
    //TextWriter(System.out).write(ds)
    val image = RGBImagePivot("band", 1.0, 4.0, 2.0)(ds)
    //TextWriter(System.out).write(image)
    ImageWriter("/data/modis/modisRGB.png").write(image)
  }
  
  @Test
  def geolocation() = {
    val ds = ModisGeolocationReader().getDataset
    //TextWriter(System.out).write(ds)
    /*
     * TODO: substitute into index based modis data
     * define name of index tuple in each to be "index"
     * make sure we can eval with a 2D domain efficiently
     * or just zip them together in the modis reader?
     * 
     */
  }
  
  //@Test
  def resample() = {
    /*
     * Given (longitude, latitude) provide (ix, iy)
     * modis: band -> (ix, iy) -> radiance
     * 
     * We have nc vars to get Dataset: (ix, iy) -> (longitude, latitude)
     * define this dataset from MYD03 file: "MODIS/Aqua Geolocation Fields 5-Min L1A Swath 1km"
     * TODO: use compressed lon-lat in orig data file
     * join via substitution
     *   need to be able to substiture a tuple
     *   tuple needs id
     * 
     * Could we apply CSX index -> geo in the process of binning?
     * akin to implicit conversion
     *   we have A but need B and we have implicit function A => B
     * special ModisBinResampling? seems like overkill
     * we need to do the substitution somewhere
     * would rather keep data on Cartesian grid even if it is indices
     * but doesn't really matter for now?
     *   just a diff example of how we can manage data 
     *   but couldn't make image in orig geo coords
     */
    
    val ds = ModisReader().getDataset
    
    // Define regular grid to resample onto
    val (nx, ny) = (3,4)
    val domainSet = LinearSet2D(1, 2.0, nx, 1, 2.0, ny)
    //TODO: cnsider (start, stride) vs (scale, offset)
    //domainSet.elements foreach println

    
    /*
     * TODO: define SF.resample with Resampling
     * how can we get inter/extrap for general use?
     * 
     * Resampling may or may not include interp
     * 
     * Resample as Operation
     * model will not change
     * need to apply to nested function
     *   how should API indicate that?
     */
    // Resample nested Functions
    // map over outer samples
    val resampleGrid = (sample: Sample) => sample match {
      case Sample(domain, RangeData(sf: MemoizedFunction)) =>
        val grid = BinResampling().resample(sf, domainSet)
        Sample(domain, RangeData(grid))
    }

    val data = ds.data.map(resampleGrid)
    
    val model = ds.model
    
    val md = ds.metadata //TODO: enhance, prov
    
    val ds2 = Dataset(md, model, data)
    TextWriter().write(ds2)
  }
}
