package latis.ops

import latis.model.Dataset
import latis.data._

/**
 * Given two Datasets of the same type, 
 * combine the Samples into a new Dataset.
 * Note that because a Dataset's Samples are ordered, 
 * this union can happen as the data are being streamed.
 * Note, this does not affect the model.
 */
case class Union() extends BinaryOperation {
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    
    val metadata = ds1.metadata //TODO: combine metadata, prov
    
    val model = ds1.model //TODO: ensure the models match
    
    val data: SampledFunction = {
      //TODO: delegate to "union(other)" function on SampledFunction
      /*
       * zipWithPreviousAndNext ?
       *   but need to advance one stream without the other
       * or consume and re-emit?
       */
      
      /*
       * Spark union
       * use .distinct to remove duplicates
       * sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] 
       */
      (ds1.data, ds2.data) match {
        case (rddf1: RddFunction, rddf2: RddFunction) =>
          RddFunction(rddf1.rdd.union(rddf2.rdd).distinct.sortBy(identity))
        case _ => ??? //TODO: only support Spark RDDs for now
      }
    }
    
    
    Dataset(metadata, model, data)
  }
}