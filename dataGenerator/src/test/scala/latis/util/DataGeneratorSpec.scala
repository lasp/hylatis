package latis.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

import latis.data.Real
import latis.data.Sample

class DataGeneratorSpec extends FlatSpec with PropertyChecks with Matchers {

  private def getValues(samples: Iterator[Sample]): List[Double] =
    samples.map {
      case Sample(_, List(Real(v))) => v
    }.toList

  "The data generator" should "have the correct base image" in {
    // Generate a 2x2x1 image with a single partition.
    val gen = new DataGenerator(2, 2, 1, 1)

    val values: List[Double] = getValues(gen.forPartition(0))

    values should be (List(3.0, 2.0, 4.0, 1.0))
  }

  it should "scale along the Z dimension" in {
    // Generate a 1x1x3 image with a single partition.
    val gen = new DataGenerator(1, 1, 3, 1)

    val values: List[Double] = getValues(gen.forPartition(0))

    values should be (List(0.0, 0.5, 1.0))
  }

  it should "preserve total voxel count when partitioning" in {
    forAll { (nX: SPInt, nY: SPInt, nZ: SPInt, nP: SPInt) =>
      val gen = new DataGenerator(nX.value, nY.value, nZ.value, nP.value)

      val expectedTotal = nX.value * nY.value * nZ.value
      val total = Iterator.tabulate(nP.value) {
        gen.forPartition(_).size
      }.sum

      total should be (expectedTotal)
    }
  }
}
