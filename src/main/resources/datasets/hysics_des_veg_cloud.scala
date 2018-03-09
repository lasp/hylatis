package datasets

import latis.metadata._
import latis.reader._
import latis.ops._
import latis.model._

/*
 * Full data cube for hysics_des_veg_cloud
 * need to build dataset from data files (local or s3)
 *   then "write" to spark
 * need diff dataset backed by spark, SparkDataFrameAdapter
 *   use separate catalogs for namespace?
 *     local, s3, spark
 * 
 * 
 * file join
 * each image file:
 *   matrix: (row, column) -> value
 *   => (i, k) -> f  via rename only
 * join with new dimension
 *   j -> (i, k) -> f
 *   (j,i) -> k -> f
 *   => (y, x) -> w -> f  via op
 *   => (lon, lat) -> w -> f  via op
 *   => index -> (lon, lat, w, f)
 *   
 * file join adapter
 * iterative adapter with each record being a Dataset!
 * 
 *   
 */

object hysics_des_veg_cloud extends DatasetDescriptor {
  
  val metadata = Metadata(
    "id" -> "hysics_des_veg_cloud",
    "history" -> s"Constructed at ${System.currentTimeMillis}"
  )
  
  val model = ??? //TODO: initial or final? probably final - used by file join
  
  val adapter = ???
}

//  Metadata("id" -> "hysics")
//)(
//  Function(Metadata("id" -> "cube"))(
//    Tuple(Integer(id = "y"), Integer(id = "x"), Real(id = "wavelength")),
//    Real(id = "value")
//  )
//)(
//  SparkDataFrameAdapter("hysics")
//)(
//  Operations()
//)
