import io.findify.s3mock.S3Mock
import latis.util.LatisConfig


object RunS3Mock extends App {
  
  for {
    port <- LatisConfig.getInt("hylatis.s3mock.port")
    dir  <- LatisConfig.get("hylatis.s3mock.dir")
  } yield S3Mock(port = port, dir = dir).start
  
}