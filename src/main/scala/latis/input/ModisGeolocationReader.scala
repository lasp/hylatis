package latis.input

import latis.model._
import latis.metadata.Metadata

import java.net.URI

case class ModisGeolocationReader() extends DatasetReader {
  //TODO: define as Dataset, need a Dataset trait instead of case class
  //or object value, not class
  
  //TODO: get from config, or construct reader with URI
  val uri: URI = new URI("/data/modis/MYD03.A2014230.1940.061.2018054000543.hdf")
    
  val metadata = Metadata("modis_geolocation")
  
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
    val data = adapter(uri)
    Dataset(metadata, model, data)
  }
}
