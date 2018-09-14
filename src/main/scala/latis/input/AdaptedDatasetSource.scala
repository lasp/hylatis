package latis.input

import latis.model._
import latis.ops.Operation
import latis.metadata._
import java.net.URI

/**
 * DatasetSource that uses an Adapter to get data from the source.
 */
trait AdaptedDatasetSource extends DatasetSource {
  
  /**
   * Location of the data source.
   */
  def uri: URI
  
  /**
   * Global metadata.
   */
  def metadata: Metadata = Metadata("id" -> uri.getPath)
  
  /**
   * Data model including metadata.
   */
  def model: DataType
  
  /**
   * Adapter to provide Data from the data source.
   */
  def adapter: Adapter
  
  /**
   * Predefined Operations to be applied to the Dataset.
   */
  def processingInstructions: Seq[Operation] = Seq.empty
  
  /**
   * Construct a Dataset by delegating to the Adapter.
   * Offer the processing instructions and given operations to the adapter
   * to apply then apply the rest.
   */
  def getDataset(ops: Seq[Operation]): Dataset = {
    // Offer the operations to the adapter to handle. 
    // Partition by what it handles.
    val (handledOps, unhandledOps) = 
      (processingInstructions ++ ops).partition(adapter.handleOperation(_))
    
    // Apply the Adapter to the given resource to get the Data.
    val data = adapter(uri)
    
    // An Adapter can only provide Data
    // so apply the "handled" operations to the model.
    //TODO: should an operation modify the global metadata other than the prov?
    val newModel = handledOps.foldLeft(model)((md, op) => op.applyToModel(md))
    
    // Construct the Dataset so far.
    val dataset = Dataset(metadata, newModel, data)
    
    // Apply the rest of the operations to the Dataset.
    unhandledOps.foldLeft(dataset)((ds, op) => op(ds))
  }
}