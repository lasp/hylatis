package datasets

import latis.model._
import latis.reader.DatasetDescriptor
import latis.metadata.Metadata
import ImplicitConversions._
import latis.data._
import latis.reader.IterativeAdapter

object hysics_des_veg_cloud_image_files extends DatasetDescriptor {
    
  val metadata = Metadata(
    "id" -> "hysics_des_veg_cloud_image_files",
    "baseURL" -> "file:/data/hysics",
    "history" -> s"Constructed at ${System.currentTimeMillis}"
  )

  val model = Function("f")(
    Integer(id="i"),
    Text(id="file")
  )
  
  val adapter = new IterativeAdapter[Int] {
    def model: Variable = hysics_des_veg_cloud_image_files.model

    def recordIterator = (1 to 4200).iterator
    
    def makeDatum(scalar: Scalar, i: Int): Option[Datum] = scalar.id match {
      case "i"    => Some(Datum(i))
      case "file" => Some(Datum(f"des_veg_cloud/img$i%04d.txt"))
    }
    
    def close = {}
  }

}