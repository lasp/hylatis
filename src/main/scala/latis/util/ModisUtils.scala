package latis.util

import java.net.URI

import latis.data.ArrayFunction2D
import latis.dataset.Dataset
import latis.input.ModisGeolocationReader

object ModisUtils {

  def geoCSX: Dataset = {
    val uri = new URI(
      LatisConfig.getOrElse("hylatis.modis.geoloc.uri", "s3://hylatis-modis/MYD03.A2014230.1945.061.2018054000612.hdf")
    )
    ModisGeolocationReader
      .read(uri)
      .restructureWith(ArrayFunction2D)
  }

}
