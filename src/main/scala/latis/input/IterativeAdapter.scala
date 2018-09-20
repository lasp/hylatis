package latis.input

import latis.data._
import java.net.URI
import latis.data.Sample

/**
 * An IterativeAdapter uses record semantics to read data.
 */
trait IterativeAdapter[R] extends Adapter {
  
  /**
   * Provide an iterator of records.
   */
  def recordIterator(uri: URI): Iterator[R] 
  
  /**
   * Optionally parse a record into a Sample
   */
  def parseRecord(r: R): Option[Sample]
  
  /**
   * Implement the Adapter interface using record semantics.
   * Note that this approach is limited by TraversableOnce
   * so this creates a StreamingFunction that is not evaluatable.
   */
  def apply(uri: URI): SampledFunction = 
    StreamingFunction(recordIterator(uri).flatMap(parseRecord(_)))

}