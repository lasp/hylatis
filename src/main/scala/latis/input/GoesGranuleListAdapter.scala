package latis.input

import java.net.URI

import latis.data._
import fs2._
import cats.effect.IO

class GoesGranuleListAdapter extends Adapter {
  def apply(uri: URI): SampledFunction = {
    val base = uri.toString //defined in goes_image_files.fdml

    // Wavelengths from https://www.ncdc.noaa.gov/data-access/satellite-data/goes-r-series-satellites/glossary converted to nm
    // URLs for 00Z from http://s3.amazonaws.com/noaa-goes16/?prefix=ABI-L1b-RadF/2018/230/17/.
    // Use only the channels with the same size images (5424 x 5424)
    val samples = Seq(
      //Sample(DomainData(470.0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C01_G16_s20182301700501_e20182301711267_c20182301711302.nc")), //10848	0.47
      //Sample(DomainData(640.0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C02_G16_s20182301700501_e20182301711267_c20182301711300.nc")), //21696	0.64
      //Sample(DomainData(860.0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C03_G16_s20182301700501_e20182301711267_c20182301711307.nc")), //10848	0.86
      Sample(DomainData(1370.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C04_G16_s20182301700501_e20182301711267_c20182301711292.nc")), //5424	1.37
      //Sample(DomainData(1600.0),RangeData(s"$base/OR_ABI-L1b-RadF-M3C05_G16_s20182301700501_e20182301711267_c20182301711307.nc")), //10848	1.6
      Sample(DomainData(2200.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C06_G16_s20182301700501_e20182301711273_c20182301711299.nc")), //5424	2.2
      Sample(DomainData(3900.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C07_G16_s20182301700501_e20182301711279_c20182301711312.nc")), //5424	3.9
      Sample(DomainData(6200.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C08_G16_s20182301700501_e20182301711267_c20182301711312.nc")), //5424	6.2
      Sample(DomainData(6900.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C09_G16_s20182301700501_e20182301711273_c20182301711336.nc")), //5424	6.9
      Sample(DomainData(7300.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C10_G16_s20182301700501_e20182301711279_c20182301711332.nc")), //5424	7.3
      Sample(DomainData(8400.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C11_G16_s20182301700501_e20182301711267_c20182301711330.nc")), //5424	8.4
      Sample(DomainData(9600.0),  RangeData(s"$base/OR_ABI-L1b-RadF-M3C12_G16_s20182301700501_e20182301711273_c20182301711335.nc")), //5424	9.6
      Sample(DomainData(10300.0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C13_G16_s20182301700501_e20182301711279_c20182301711338.nc")), //5424	10.3
      Sample(DomainData(11200.0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C14_G16_s20182301700501_e20182301711267_c20182301711337.nc")), //5424	11.2
      Sample(DomainData(12300.0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C15_G16_s20182301700501_e20182301711273_c20182301711337.nc")), //5424	12.3
      Sample(DomainData(13300.0), RangeData(s"$base/OR_ABI-L1b-RadF-M3C16_G16_s20182301700501_e20182301711279_c20182301711333.nc"))  //5424	13.3
    )
    
    SampledFunction(samples)
  }
}