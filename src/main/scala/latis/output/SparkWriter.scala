package latis.output

import latis.model._
import latis.util.SparkUtils._
import latis.util.StreamUtils._
import latis.data.Sample
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/*
 * Writing a large number of samples to spark like this is not a good idea.
 * Instead, we write the granule list dataset and use an operation to read each URI.
 * See HylatisServer.init.
 */
/**
 * Put a LaTiS Dataset into a Spark RDD by Samples.
 * Because the Dataset's sequence of Samples could be quite long,
 * buffered sets of Samples will be parallelized into RDDs
 * then unioned into the final RDD.
 */
case class SparkWriter() {
  
  def write(dataset: Dataset): Unit = {
    val sc = sparkContext
    
//    // Number of samples to parallelize at a time
//    val bufferSize = 100000 //TODO: tune
//
//    // Group the Dataset's samples into "buffers" 
//    // which each get parallelized into an RDD.
//    // Use fold to union the RDDs into the final RDD.
//    val emptyRDD: RDD[Sample] = sc.parallelize(Seq.empty)
//    val rdd = dataset.samples.grouped(bufferSize).foldLeft(emptyRDD){ (rdd, ss) =>
//      val tmprdd = sc.parallelize(ss)
//      tmprdd.count() // Force it to load the data.
//      rdd union tmprdd
//    } 

    //TODO: note, we had to force in both places with "count" for this to show up in storage (http://localhost:4040/storage/)
    val samples = unsafeStreamToSeq(dataset.data.streamSamples) //unsafe
    val rdd = sc.parallelize(samples)
    rdd.count()
    // Cache this RDD in memory for later use.
    rdd.cache.setName(dataset.id).count()
 
    /*
     * TODO: since this isn't the usual "write",
     * consider creating a new Dataset that has a handle to the RDD
     * instead of using the cache?
     * 
     * Or should we consider spark just another caching option?
     * use property to specify what cache to use
     * dataset.cache => new Dataset instance with spark adapter instead of orig adapter
     * Should dataset.cache return a new Dataset?
     *   could then change to spark adapter
     *   not quite like "force" which effectively evals the lazy stuff
     * 
     * Note that cached datasets will show up in the web UI.
     * 
     * We may also end up loading data into spark RDD via other mechanism so 
     *   we still need to be able to find the RDD by name
     */
  }
}
