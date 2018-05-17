import io.findify.s3mock.S3Mock
import latis.util.LatisProperties


object RunS3Mock extends App {
  
  for {
    port <- LatisProperties.get("s3mock.port")
    dir  <- LatisProperties.get("s3mock.dir")
  } yield S3Mock(port = port.toInt, dir = dir).start
  
}