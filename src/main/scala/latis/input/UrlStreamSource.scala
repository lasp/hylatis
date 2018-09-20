package latis.input

import java.net.URL
import java.io.InputStream

/**
 * Create a StreamSource from a URL.
 */
case class UrlStreamSource(val url: URL) extends StreamSource {
  
  def getStream: InputStream = url.openStream
  
}