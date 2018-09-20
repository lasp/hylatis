package latis.input

import latis.ops.Operation
import latis.data.SampledFunction
import java.net.URI

/**
 * An Adapter provides access to data in the form of a SampledFunction
 * given a URI. It may also apply Operations to the data.
 */
trait Adapter {
  
  def apply(uri: URI): SampledFunction
  
  def handleOperation(op: Operation): Boolean = false
  
}