package latis.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import latis.data.Data
import latis.data.Sample

/**
 * Generates a partitioned three-dimensional data cube.
 *
 * The cube will have dimensions nX ⨯ nY ⨯ nZ where the last dimension
 * (Z) varies fastest. The intended interpretation is that the X-Y
 * plane represents an image and the Z dimension represents a
 * spectrum.
 *
 * For an X-Y slice at Z=z (nZ > 1), the values of the voxels are:
 *
 * <pre>
 *   nY +-----------+-----------+
 *      | 2z/(nZ-1) |  z/(nZ-1) |
 * nY/2 +-----------+-----------+
 *      | 3z/(nZ-1) | 4z/(nZ-1) |
 *    0 +-----------+-----------+
 *      0         nX/2         nX
 * </pre>
 *
 * If nZ=1, the base image is just the numerators of the coefficients
 * of z given above.
 *
 * @constructor Create a generator given size and partition parameters.
 * @param nX size in X dimension (nX > 0)
 * @param nY size in Y dimension (nY > 0)
 * @param nZ size in Z dimension (nZ > 0)
 * @param nP number of partitions (nP > 0)
 */
class DataGenerator(nX: Int, nY: Int, nZ: Int, nP: Int) {
  require(nX > 0, "nX must be a positive integer")
  require(nY > 0, "nY must be a positive integer")
  require(nZ > 0, "nZ must be a positive integer")
  require(nP > 0, "nP must be a positive integer")

  private val nVoxels: Int = nX * nY * nZ

  /** Number of voxels that can be evenly distributed per partition. */
  private val nVoxelsPerPartition: Int = nVoxels / nP

  /** Number of voxels that can't be distributed evenly. */
  private val nExtraVoxels: Int = nVoxels % nP

  /**
   * Determines the number of voxels to place in a given partition.
   *
   * If this is partition 0 through N-1 (where N is the number of
   * voxels that don't divide evenly), add an extra voxel.
   */
  private def nVoxelsForPartition(p: Int): Int =
    nVoxelsPerPartition + (if (p < nExtraVoxels) 1 else 0)

  /** Computes the value of a voxel at the given coordinates. */
  private def voxel(x: Int, y: Int, z: Int): Double = {
    // See diagram in class docs.
    val base = if (x >= nX/2 && y >= nY/2) {
      1
    } else if (x < nX/2 && y >= nY/2) {
      2
    } else if (x < nX/2 && y < nY/2) {
      3
    } else 4

    val scale = if (nZ > 1) z/(nZ-1).toDouble else 1

    base * scale
  }

  /**
   * Converts a one-dimensional index to a three-dimensional index
   * where the last dimension varies fastest.
   */
  private def to3D(i: Int): (Int, Int, Int) = {
    val x = i / (nY * nZ)
    val y = (i / nZ) % nY
    val z = i % nZ

    (x, y, z)
  }

  /**
   * Generates samples for a given partition.
   *
   * @param p index of partition to generate (0 <= p < nP)
   */
  def forPartition(p: Int): Iterator[Sample] = {
    require(p >= 0, "partition number must be non-negative")
    require(p < nP, "p must be smaller than the number of partitions")

    val n = nVoxelsForPartition(p)
    val start = List.tabulate(p)(nVoxelsForPartition).sum

    Iterator.range(start, start + n).map { i =>
      val (x, y, z) = to3D(i)
      val r = voxel(x, y, z)

      Sample(List(Data(x), Data(y), Data(z)), List(Data(r)))
    }
  }
}

object DataGenerator {

  /**
   * Creates an RDD with the generated data cube.
   *
   * @param nX size in X dimension (nX > 0)
   * @param nY size in Y dimension (nY > 0)
   * @param nZ size in Z dimension (nZ > 0)
   * @param nP number of partitions (nP > 0)
   * @params ctx the Spark context the RDD will belong to
   */
  def generateRdd(nX: Int, nY: Int, nZ: Int, nP: Int, ctx: SparkContext): RDD[Sample] = {
    val gen = new DataGenerator(nX, nY, nZ, nP)

    // End of range is exclusive.
    ctx.range(0, nX.toLong * nY * nZ, numSlices=nP).mapPartitionsWithIndex {
      case (p, _) => gen.forPartition(p)
    }
  }
}
