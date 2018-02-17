import org.geotools.referencing.CRS
import org.geotools.referencing.GeodeticCalculator
import org.geotools.geometry.jts.JTS
import com.vividsolutions.jts.geom.Coordinate
import latis.reader.DatasetSource
import latis.writer.Writer

object TestGeoLocation extends App {
  /*
   * dx = 13.16 m/px along slit
   * 
   */
  
  //val ds = DatasetSource.fromName("hysics_des_veg_cloud_gps").getDataset()
  //Writer().write(ds)
  
  /*
   * Calculate distance between each image lon,lat location.
   * https://www.nhc.noaa.gov/gccalc.shtml
   *  
   * Assume equal spacing of images? avg then use a dy
   * But assumes linear track.
   */
  val crs = CRS.decode("EPSG:4326")
  val gc = new GeodeticCalculator(crs)
  //val p1 = new Coordinate(3.4714011e+01,  -1.0821368e+02)
  //val p2 = new Coordinate(3.4714015e+01,  -1.0821369e+02)
  val p1 = new Coordinate(34.714011, -108.21368) //lat,lon
  val p2 = new Coordinate(34.731971, -108.2499)
  gc.setStartingPosition( JTS.toDirectPosition( p1, crs ) )
  gc.setDestinationPosition( JTS.toDirectPosition( p2, crs ) )
    
  val d = gc.getOrthodromicDistance() //meters
  //val d = JTS.orthodromicDistance(p1, p2, crs)
  println(d)
  
}