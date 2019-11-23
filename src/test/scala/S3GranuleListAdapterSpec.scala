import java.net.URI

import scala.util.matching.Regex

import org.scalatest._

import latis.input.AdaptedDatasetReader
import latis.input.S3GranuleListAdapter
import latis.metadata.Metadata
import latis.model._
import latis.util.AWSUtils
import latis.util.S3MockFixture

// Populates S3Mock for this particular suite.
trait PopulateS3Mock extends BeforeAndAfterAll { this: Suite =>

  override def beforeAll(): Unit = {
    import S3GranuleListAdapterSpec._

    val client = AWSUtils.s3Client.get

    client.createBucket(bucket)
    objects.foreach(name => client.putObject(bucket, name, ""))

    super.beforeAll()
  }

  override def afterAll(): Unit = super.afterAll()
}

class S3GranuleListAdapterSpec
    extends FlatSpec
    with PopulateS3Mock
    with S3MockFixture {
  import S3GranuleListAdapterSpec._

  "The S3 granule list adapter" should "list S3 objects with a given prefix" in {
    val reader = S3GranuleListAdapterSpec.reader(
      new URI(s"s3://$bucket/test/"),
      "".r,
      Function(
        Scalar(Metadata("id" -> "index", "type" -> "long")),
        Scalar(Metadata("id" -> "key", "type" -> "string"))
      )
    )

    //reader.getDataset match {
    //  case Dataset(_, _, f) =>
    //    assert(f.unsafeForce.samples.length === 3)
    //}
  }

  it should "extract values from records" in {
    val reader = S3GranuleListAdapterSpec.reader(
      new URI(s"s3://$bucket/test/"),
      """test/test(?<num>\d{3})""".r,
      Function(
        Scalar(Metadata("id" -> "index", "type" -> "long")),
        Tuple(
          Scalar(Metadata("id" -> "num", "type" -> "int")),
          Scalar(Metadata("id" -> "key", "type" -> "string"))
        )
      )
    )

    //reader.getDataset match {
    //  case Dataset(_, _, f) =>
    //    val samples = f.unsafeForce.samples
    //
    //    assert(samples.length === 3)
    //    assert(samples(0)._1(0) === 0)
    //}
  }
}

object S3GranuleListAdapterSpec {
  val bucket = "s3-granule-list-adapter-test"
  val objects = List("test/test000", "test/test001", "test/test002", "fake")

  def reader(u: URI, p: Regex, m: DataType): AdaptedDatasetReader = ???
    //new AdaptedDatasetReader {
    //  def uri: URI = u
    //  def model: DataType = m
    //  def adapter = new S3GranuleListAdapter(p, m)
    //}
}
