package latis.input

import latis.model._
import latis.metadata.Metadata

import java.net.URI
import latis.util.LatisConfig
import latis.data.ArrayFunction2D

case class ModisGeolocationReader() extends DatasetReader {
  //TODO: define as Dataset, need a Dataset trait instead of case class
  //or object value, not class
  
  /**
   * Get the URI for the MODIS data file to read.
   */
  val uri: URI = LatisConfig.get("hylatis.modis.geoloc.uri") match {
    case Some(s) => new URI(s) //TODO: invalid URI
    case _ => ??? //TODO: uri not defined
  }
  
  val metadata = Metadata("modis_geolocation")
  
  // (ix, iy) -> (longitude, latitude)
  val model = Function(
    Tuple(Scalar("ix"), Scalar("iy")),
    Tuple(
      Scalar(Metadata(
          "id" -> "longitude", 
          "type" -> "float", 
          "origName" -> "MODIS_Swath_Type_GEO/Geolocation_Fields/Longitude")
      ), 
      Scalar(Metadata(
          "id" -> "latitude", 
          "type" -> "float", 
          "origName" -> "MODIS_Swath_Type_GEO/Geolocation_Fields/Latitude")
      )
    )
  )
  
  val adapter = NetcdfAdapter(model)
  
  def getDataset: Dataset = {
    // Use ArrayFunction2D to optimize evaluation by index domain values
    val data = ArrayFunction2D.restructure(adapter(uri))
    Dataset(metadata, model, data)
  }
}
