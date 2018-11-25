package latis.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkUtils {
  
  lazy val sparkContext = makeSparkContext
  
  private def makeSparkContext: SparkContext = {
    // Create Spark config based on defined properties
    var sconf = new SparkConf()
    LatisProperties.get("spark.master").foreach(master => sconf = sconf.setMaster(master))
    LatisProperties.get("spark.app").foreach(app => sconf = sconf.setAppName(app))
    LatisProperties.get("spark.default.parallelism") foreach { n =>
      sconf = sconf.set("spark.default.parallelism", n)
    }
    
    // Register Kryo serializable classes
    sconf.registerKryoClasses(Array(
//      classOf[latis.data.ScalarData[_]]
    ))
    
    new SparkContext(sconf)
  }
      
  //hopefully not needed now that we are using RddFunction to handle the RDD.
//  def getRDD(name: String): Option[RDD[_]] = {
//    //getSparkSession.sparkContext.getPersistentRDDs.map(_._2).find(_.name == name)
//    rddCache.get(name)
//  }
//  
//  def cacheRDD(name: String, rdd: RDD[_]) = rddCache += (name -> rdd)
//  
//  private val rddCache = scala.collection.mutable.Map[String, RDD[_]]()
}