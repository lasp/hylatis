package latis.util

import org.apache.spark.Partitioner

// Only need to partition after the groupBy:
//
// (x, y) -> w -> f

class HylatisPartitioner(partitions: Int) extends Partitioner {

  val nx: Int = 2 //480
  // imageCount samples the images rather than taking the first N
  val ny: Int = 2 //4200

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case d: Seq[_] if d.length == 2 =>
      val x: Integer = d(0).asInstanceOf[Integer]
      val y: Integer = d(1).asInstanceOf[Integer]

      val nPerPartition = (nx * ny).toDouble / numPartitions
      val i = y + ny * x  //TODO: need indexOf?

      ((i).toDouble / nPerPartition).toInt
    case _ => throw new RuntimeException(
      "Unsupported domain type."
    )
  }
}
