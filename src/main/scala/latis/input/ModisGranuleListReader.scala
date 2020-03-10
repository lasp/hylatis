package latis.input

import java.net.URI

import latis.data._
import latis.dataset._
import latis.metadata._
import latis.model._
import latis.util.LatisConfig

object ModisGranuleListReader extends DatasetReader {
  
  val model: Function = Function(
    Scalar(Metadata("id" -> "band", "type" -> "double")),
    Scalar(Metadata("id" -> "uri", "type" -> "string"))
  )

  def read(uri: URI): Dataset = {
    val metadata =  Metadata("modis")
    val data = getData(uri)
    new MemoizedDataset(metadata, model, data)
  }
  
  def getData(uri: URI): MemoizedFunction = {
    val stride = LatisConfig.getOrElse("hylatis.modis.stride", "1")
    val file = uri.toString
    val samples = Seq(
      Sample(DomainData(1.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_250_Aggr1km_RefSB#(0,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(2.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_250_Aggr1km_RefSB#(1,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(3.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB#(0,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(4.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB#(1,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(5.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB#(2,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(6.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB#(3,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(7.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_500_Aggr1km_RefSB#(4,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(8.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(0,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(9.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(1,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(10.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(2,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(11.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(3,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(12.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(4,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(13.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(5,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(13.5), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(6,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(14.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(7,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(14.5), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(8,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(15.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(9,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(16.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(10,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(17.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(11,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(18.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(12,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(19.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(13,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(26.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_RefSB#(14,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(20.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(0,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(21.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(1,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(22.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(2,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(23.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(3,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(24.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(4,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(25.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(5,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(27.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(6,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(28.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(7,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(29.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(8,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(30.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(9,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(31.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(10,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(32.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(11,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(33.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(12,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(34.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(13,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(35.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(14,0:2029:$stride,0:1353:$stride)")),
      Sample(DomainData(36.0), RangeData(s"$file!/MODIS_SWATH_Type_L1B/Data_Fields/EV_1KM_Emissive#(15,0:2029:$stride,0:1353:$stride)")),
    )

    //val samples = LatisConfig.getInt("hylatis.modis.nbands") match {
    //  case Some(n) => allSamples.take(n)
    //  case None    => allSamples
    //}
    
    SampledFunction(samples)
  }

}
