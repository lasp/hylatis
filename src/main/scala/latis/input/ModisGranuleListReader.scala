package latis.input

import latis.data._
import latis.metadata._
import latis.model._
import latis.util.LatisConfig

case class ModisGranuleListReader() extends DatasetReader {
  
  val model = Function(
    Scalar(Metadata("id" -> "band", "type" -> "double")),
    Scalar(Metadata("id" -> "uri", "type" -> "text"))
  )
  
  val data = {
    val file = LatisConfig.get("hylatis.modis.uri").get
    val samples = Seq(
      Sample(DomainData(1.0), RangeData(s"$file,MODIS_SWATH_Type_L1B/Data_Fields/EV_250_Aggr1km_RefSB,0")),
      Sample(DomainData(2.0), RangeData(s"$file,MODIS_SWATH_Type_L1B/Data_Fields/EV_250_Aggr1km_RefSB,1")),
      Sample(DomainData(3.0), RangeData(s"$file,MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB,0")),
      Sample(DomainData(4.0), RangeData(s"$file,MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB,1")),
      Sample(DomainData(5.0), RangeData(s"$file,MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB,2")),
      Sample(DomainData(6.0), RangeData(s"$file,MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB,3")),
      Sample(DomainData(7.0), RangeData(s"$file,MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB,4"))
    )
    SampledFunction.fromSeq(samples)
  }
  
  def getDataset = Dataset(Metadata("modis_granules"), model, data)
}