import latis._
import latis.input._
import latis.output._
import java.io.FileOutputStream


object TestHysicsReader extends App {
  
  val ds = HysicsLocalReader().getDataset()
  
  /*
   * TODO: put on regular lon,lat grid
   * We have a grid in some x,y space that is rotated relative to the lon,lat coordinate system.
   * By the time we have subset on lon and lat, the corners have been truncated so we no longer have complete x,y rows.
   * We can put the data in a Function1D with (lon,lat) keys.
   * However, nearest samples from that sorted map won't represent closeness in 2D.
   * We can't make a Function2D because we don't have a Cartesian grid of lon,lat.
   * What if we had a Function2D of the original x,y (or i,j) grid?
   *   use (lon,lat) -> (x,y) for selections? not directly since not independent
   *   regrid with a domainSet of (lon,lat)
   *   evaluate via function composition
   * 
   * Keep data on x,y grid in spark
   * define (continuous) function via composition
   * We have xyToGeo; need geoToXY (include rotation)
   *   what about geoToIndex? 
   *   could use Array2DFunction
   *   not good for interpolation: di != dj
   * 
   */
}