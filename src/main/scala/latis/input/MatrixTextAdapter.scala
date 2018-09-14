package latis.input

import latis.data._
import latis.metadata._
import java.io._
import java.net.URI
import latis.data.Text

/**
 * Read a text file which represents a matrix. 
 * This differs from tabular text data in that columns
 * represent a dimension (i.e. domain variable) and not 
 * a tuple (i.e. range variables).
 * Unlike gridded data, the column dimension varies fastest
 * and the row dimension varies from top to bottom. The same 
 * row/column semantics apply to Image data.
 */
case class MatrixTextAdapter(config: AsciiAdapter.Config) extends Adapter {

  //TODO: manage resource
  def apply(uri: URI): Data = {
    val is = URIResolver.resolve(uri).getStream //TODO: getReader? getLines?
    val reader = new BufferedReader(new InputStreamReader(is))
    val delimiter = config.delimiter

    import collection.JavaConverters._
    val lines: Array[String] = reader.lines.iterator.asScala.toArray
    val arrays: Array[Array[Data]] = lines.map(_.split(delimiter).map(Text(_).asInstanceOf[Data])) //TODO: avoid cast
    ArrayFunction2D(arrays)
  }
}

object MatrixTextAdapter {
  
  def apply(): MatrixTextAdapter = new MatrixTextAdapter(AsciiAdapter.Config())
}