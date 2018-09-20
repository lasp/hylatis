package latis.input

import java.net.URI
import java.io.InputStream

/**
 * Trait for a data source that provides an InputStream.
 */
trait StreamSource {
  
  def getStream: InputStream
  //TODO: handle error, Option or Try?
  //TODO: auto closable 
  
  //TODO: getLines?
  
}