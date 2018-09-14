package latis.input

import java.net.URL
import java.io.InputStream

case class UrlStreamSource(val url: URL) extends StreamSource {
  
  def getStream: InputStream = url.openStream
  //TODO: handle error, Option or Try?
  //TODO: auto closable 
  
}