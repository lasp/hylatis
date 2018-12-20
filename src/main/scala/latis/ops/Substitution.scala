package latis.ops

import latis.model._
import latis.data._

case class Substitution() extends BinaryOperation {
  //TODO: or UnaryOpertaion constructed with Function to substitute
  //  via partial application?
  //TODO: explore consequences of complex types
  //  this generally assumes ds2 is scalars a -> b but could be any type
  
  /**
   * Apply second Dataset to the first replacing the variable matching the domain
   * variable of the second with its range variable.
   */
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    val model = applyToModel(ds1.model, ds2.model)
    
    val data = applyToData(ds1, ds2)
    
    //TODO: update Metadata
    Dataset(ds1.metadata, model, data)
  }
  
  
  def applyToData(ds1: Dataset, ds2: Dataset): SampledFunction = {
    // Get the dt2 domain variable id and range variable
    val vid = ds2.model match {
      case Function(d, r) => d.id
      case _ => ??? //TODO invalid dataset type for ds2
    }
    
    // Get the sample path of the ds2 domain variable in ds1.
    // Assume that it is not in a nested Function.
    val pos: SamplePosition = ds1.model.getPath(vid) match {
      case Some(p) => p.head
      case None => ??? //error, variable not found in ds1
    }
    
    // Get the SampledFunction of ds2
    val sf = ds2.data
    
    // Make a function to modify a ds1 Sample by replacing the value
    // from evaluating ds2 with the value of the matching variable in ds1
    val f: Sample => Sample = (s: Sample) => {
      val v1 = s.getValue(pos).get //TODO: assumes variable exists and is not in a nested Function
      val v2 = sf(DomainData(v1)).unsafeForce.head.range.head //evaluate new value
      s.updateValue(pos, v2)
    }
    
    ds1.data.map(f)
  }
  
  def applyToModel(dt1: DataType, dt2: DataType): DataType = {
    //TODO: error if no variable in dt1 matching dt2 domain
    
    // Get the dt2 domain variable id and range variable
    val (vid, range) = dt2 match {
      case Function(d, r) => (d.id, r)
      case _ => ??? //TODO invalid dataset type for ds2
    }
    
    // Traverse dt1 and replace the type matching the dt2 domain
    // with the new type from the dt2 range.
    //TODO: support aliases?
    dt1 map {
      case dt if (dt.id == vid) => range  //replace matching variable
      case dt => dt  //no match, keep original variable
    }
  }
}