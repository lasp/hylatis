package datasets

import latis.model._
import latis.reader.DatasetDescriptor
import latis.metadata.Metadata
import ImplicitConversions._
import latis.data._
import latis.reader.IterativeAdapter
import java.net.URL
import latis.reader.AsciiAdapter

object hysics_des_veg_cloud_gps extends DatasetDescriptor {
    
  val metadata = Metadata(
    "id" -> "hysics_des_veg_cloud_gps",
    "history" -> s"Constructed at ${System.currentTimeMillis}"
  )

  val model = Function("f")(
    Real(id="time"),
    Tuple(Real("latitude"), Real("longitude"))
  )
  
  val location = new URL("file:/data/hysics/des_veg_cloud/gps_info.txt")
  val adapter = AsciiAdapter(model, location, delimiter="""\s+""")

}