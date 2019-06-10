package latis.input

import java.net.URI

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

import cats.effect.IO
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.iterable.S3Objects
import fs2.Stream

import latis.data._
import latis.model._
import latis.util.AWSUtils

/**
 * An adapter for listing the contents of an S3 bucket.
 *
 * The granule list will be a function of index to the key of each S3
 * object (if a scalar named "key" is present) and whatever additional
 * information is pulled from the key using the given pattern.
 *
 * The URI provided to this adapter must be a S3 URI. The s3, http,
 * and https schemes are allowed. The URI should contain a prefix
 * rather than a full key.
 *
 * The model provided to this adapter must be a function with a single
 * scalar of type "long" in the domain and a scalar or tuple in the
 * range. A scalar of type "string" named "key" in the range will
 * receive the key of the S3 object, but this scalar is not required.
 *
 * The pattern provided to this adapter must have named capture groups
 * with names corresponding to the names of each scalar in the range
 * besides "key".
 */
class S3GranuleListAdapter(pattern: Regex, model: DataType)
    extends StreamingAdapter[(String, Long)] {

  def recordStream(uri: URI): Stream[IO, (String, Long)] = {
    val keys: Stream[IO, String] = for {
      s3Uri  <- Stream.eval(getS3URI(uri))
      bucket <- Stream.eval(getBucket(s3Uri))
      prefix <- Stream.eval(getKey(s3Uri))
      client <- Stream.eval(getS3Client)
      obj    <- listS3Objects(client, bucket, prefix)
      key     = obj.getKey()
    } yield key

    keys.zipWithIndex
  }

  def parseRecord(r: (String, Long)): Option[Sample] =
    r match {
      case (key, index) =>
        pattern.findFirstMatchIn(key).flatMap { mtch =>
          // This match should be safe because we checked that the
          // model has this structure in the apply method.
          val rtypes: Vector[Scalar] = model match {
            case Function(_, r) => r.getScalars
          }

          val rdata: Option[Vector[Any]] = rtypes.traverse {
            // A scalar named "key" will get the object's key.
            case s if s.id == "key" => Option(s.parseValue(key))
            // Look for the scalar ID in the named capture groups.
            case s =>
              Try(Option(mtch.group(s.id)))
                .toOption
                .flatten
                .map(s.parseValue(_))
          }

          rdata.map(Sample(List(index), _))
        }
    }

  override def apply(uri: URI): SampledFunction = {
    // This adapter only makes sense for functions.
    val (dtypes, rtypes) = model match {
      case Function(d, r) => (d.getScalars, r.getScalars)
      case _ => throw new RuntimeException(
        "Unsupported model."
      )
    }

    // Should only have a single scalar in the domain (index).
    if (dtypes.length != 1) {
      throw new RuntimeException(
        "Domain should contain a single scalar of type \"long\"."
      )
    }

    // Domain must be a long. We've already established that
    // there's a single scalar in the domain, so getting the
    // head is safe.
    if (dtypes.head("type").map(_ != "long").getOrElse(true)) {
      throw new RuntimeException(
        "Domain should contain a single scalar of type \"long\"."
      )
    }

    // If a range scalar named "key" exists it must have type
    // "string".
    if (rtypes.find(_.id == "key").flatMap(_("type")).map(_ != "string").getOrElse(true)) {
      throw new RuntimeException(
        "Range must contain scalar named \"key\" of type \"string\"."
      )
    }

    super.apply(uri)
  }

  private def getS3URI(uri: URI): IO[AmazonS3URI] =
    IO(new AmazonS3URI(uri))

  private def getBucket(uri: AmazonS3URI): IO[String] =
    Option(uri.getBucket()) match {
      case Some(bucket) => IO.pure(bucket)
      case None => IO.raiseError(
        new RuntimeException("Could not get bucket.")
      )
    }

  private def getKey(uri: AmazonS3URI): IO[String] =
    Option(uri.getKey()) match {
      case Some(key) => IO.pure(key)
      case None => IO.raiseError(
        new RuntimeException("Could not get key.")
      )
    }

  private val getS3Client: IO[AmazonS3] =
    AWSUtils.s3Client match {
      case Some(client) => IO.pure(client)
      case None => IO.raiseError(
        new RuntimeException("Could not get S3 client.")
      )
    }

  private def listS3Objects(
    client: AmazonS3,
    bucket: String,
    prefix: String
  ): Stream[IO, S3ObjectSummary] = for {
    it   <- Stream.eval(getS3ObjectIterator(client, bucket, prefix))
    objs <- Stream.fromIterator[IO, S3ObjectSummary](it)
  } yield objs

  private def getS3ObjectIterator(
    client: AmazonS3,
    bucket: String,
    prefix: String
  ): IO[Iterator[S3ObjectSummary]] = IO {
    S3Objects.withPrefix(client, bucket, prefix).iterator.asScala
  }
}
