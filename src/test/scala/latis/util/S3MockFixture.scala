package latis.util

import io.findify.s3mock.S3Mock
import org.scalatest._

/**
 * A mixin that runs S3Mock in its in-memory mode for the duration of
 * a test suite.
 *
 * This requires that the following properties be set:
 * - s3mock.port
 *
 * Failing to start S3Mock will cause the suite to be aborted.
 */
trait S3MockFixture extends BeforeAndAfterAll { this: Suite =>

  var s3: S3Mock = null

  /** Start S3Mock. */
  override def beforeAll(): Unit = {
    val port = LatisConfig.getInt("hylatis.s3mock.port").getOrElse(
      throw new RuntimeException("Set hylatis.s3mock.port")
    )

    s3 = S3Mock(port)
    s3.start

    super.beforeAll()
  }

  /** Stop S3Mock. */
  override def afterAll(): Unit = {
    super.afterAll()

    s3.stop
  }
}
