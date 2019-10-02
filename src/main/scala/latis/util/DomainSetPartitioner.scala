package latis.util

import org.apache.spark.Partitioner
import latis.data._

case class DomainSetPartitioner(domainSet: LinearSet1D) extends Partitioner {
  // Just use the first value (dimension) in the domain
  // Make an extra partition samples that don't fall within this DomainSet.
  
  def numPartitions: Int = domainSet.length + 1
  
  def getPartition(key: Any): Int = key match {
    case DomainData(d, _*) => 
      val index = domainSet.indexOf(DomainData(d))
      if (index >= 0) index
      else numPartitions - 1 //put invalid sample in last partition, is this better than first?
  }
  
}