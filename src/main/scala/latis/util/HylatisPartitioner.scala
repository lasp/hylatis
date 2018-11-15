package latis.util

import org.apache.spark.Partitioner

// Only need to partition after the groupBy:
//
// (y, x) -> w -> f

class HylatisPartitioner(partitions: Int) extends Partitioner {

  val nx: Int = 480
  // imageCount samples the images rather than taking the first N
  val ny: Int = 4200

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case d: Array[_] if d.length == 2 =>
      val x: Integer = d(1).asInstanceOf[Integer]
      val y: Integer = d(0).asInstanceOf[Integer]

      val nPerPartition = nx * ny / numPartitions
      val i = x + nx * y

      i / nPerPartition
    case _ => throw new RuntimeException(
      "Unsupported domain type."
    )
  }
}
