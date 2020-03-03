package latis.util

import org.apache.spark.Partitioner

import latis.data._

case class HylatisPartitioner(count: Int, min: Double, max: Double) extends Partitioner {
  //TODO: any Datum, Ordering

  override def numPartitions: Int = count

  private val interval: Double = (max - min) / count

  override def getPartition(key: Any): Int = key match {
    case DomainData(Number(d), _*) =>
      if (d == max) count - 1 //max value inclusive
      else ((d - min) / interval).toInt
      //TODO: consider out of range, bin for bad data
    case _ => throw LatisException(
      "Key must be of type DomainData."
    )
  }
}
