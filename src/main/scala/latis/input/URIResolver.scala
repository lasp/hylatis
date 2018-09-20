package latis.input

import java.net.URI


object URIResolver {
  //TODO: move to NetUtils?
  
  /**
   * Given a URI, e.g. for a data source location, use the scheme
   * to create a StreamSource that can provide an InputStream for
   * that resource.
   */
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
    //TODO: sanity check that the URL is valid
  }
  
}