package latis.input

import latis.model._
import latis.metadata.Metadata

object ModisGeolocationReader extends AdaptedDatasetReader {
  
  def metadata = Metadata("modis_geolocation")
  
  // (ix, iy) -> (longitude, latitude)
  def model = Function(
    Tuple(
      Scalar(Metadata("id" -> "ix", "type" -> "int")),
      Scalar(Metadata("id" -> "iy", "type" -> "int"))
    ),
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
  
  def adapter = NetcdfAdapter(model)

}
