package latis.input

import java.net.URI

object URIResolver {
  //TODO: move to NetUtils?
  
  def resolve(uri: URI): StreamSource = uri.getScheme match {
    //TODO: get property resolver.foo.class
    case "s3" =>
      // Assumes s3://<bucket>/<key>
      val src = for {
        bucket <- Option(uri.getHost)
        key    <- Option(uri.getPath).map(_.stripPrefix("/"))
      } yield S3StreamSource(bucket, key)

      src.getOrElse {
        throw new RuntimeException("Invalid S3 URI")
      }
    case url => UrlStreamSource(uri.toURL)
  }
  
}