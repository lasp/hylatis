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
   * Resolvable identifier of the data source.
   */
  def uri: URI
  
  /**
   * Global metadata.
   * Use the path of the URI by default.
   */
  def metadata: Metadata = Metadata("id" -> uri.getPath)
  
  /**
   * Data model for the Dataset.
   */
  def model: DataType
  
  /**
   * Adapter to provide data from the data source.
   */
  def adapter: Adapter
  
  /**
   * Predefined Operations to be applied to the Dataset.
   * Default to none.
   */
  def operations: Seq[Operation] = Seq.empty
  
  /**
   * Construct a Dataset by delegating to the Adapter.
   * Offer the pre-defined and given operations to the adapter
   * to apply then apply the rest. Since Adapters only supply
   * data, operations that they handle will also be applied to 
   * the model here.
   */
  def getDataset(ops: Seq[Operation]): Dataset = {
    // Offer the operations to the adapter to handle. 
    // Partition by what it handles.
    val (handledOps, unhandledOps) = 
      (operations ++ ops).partition(adapter.handleOperation(_))
    
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