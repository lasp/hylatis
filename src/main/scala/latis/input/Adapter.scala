package latis.input

import latis.ops.Operation
import latis.data.SampledFunction
import java.net.URI

trait Adapter {
  
  /*
   * An Adapter is designed to be reusable for a given configuration.
   * Consider the GranuleJoin use case where the only thing that changes is the URI for the granule.
   * 
   * TODO: what about a Reader (e.g. NetCDF)
   * reader also needs URI so it can get metadata
   * could pass the opened source to adapter
   * but consider lazy dataset: get model/metadata eagerly but data later
   * maybe separate opens is OK, optimize later
   * better generability to think of metadata and data as separate sources
   * 
   * granule join using readers: URI list + reader
   * 
   */
  
  def apply(uri: URI): SampledFunction
  
  def handleOperation(op: Operation): Boolean = false
  
}