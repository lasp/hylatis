package latis.output

import java.io.OutputStream
import java.io.DataOutputStream

import geotrellis.raster.io.geotiff.writer.{GeoTiffWriter => GTWriter}

import latis.dataset.Dataset

class GeoTiffWriter(out: OutputStream) {
  def write(dataset: Dataset): Unit = {
    val enc           = new GeoTiffEncoder
    val dataOut       = new DataOutputStream(out)
    val geoTiff       = enc.encode(dataset).compile.toList.unsafeRunSync().head
    val geoTiffWriter = new GTWriter(geoTiff, dataOut)
    geoTiffWriter.write()
  }
}

object GeoTiffWriter {
  def apply(out: OutputStream): GeoTiffWriter =
    new GeoTiffWriter(out)
}
