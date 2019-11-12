package latis.util

import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink

/**
 * A small (<= 100) positive integer.
 *
 * This is intended for constraining the values ScalaCheck generates,
 * not for use as a subset of integers represented as a type.
 */
final case class SPInt(value: Int) extends AnyVal

object SPInt {

  implicit val spIntArb: Arbitrary[SPInt] = Arbitrary {
    Gen.choose(1, 100).map(SPInt(_))
  }

  implicit val spIntShrink: Shrink[SPInt] = Shrink {
    case SPInt(x) => Stream.iterate(x/2)(_/2).takeWhile(_ > 0).map(SPInt(_))
  }
}
