package latis.input

import latis.data._
import java.net.URI
import latis.data.Sample

trait IterativeAdapter[R] extends Adapter {
  
  def recordIterator(uri: URI): Iterator[R] 
  
  def parseRecord(r: R): Option[Sample]
  
  def apply(uri: URI): Data = 
    StreamingFunction(recordIterator(uri).flatMap(parseRecord(_)))

}