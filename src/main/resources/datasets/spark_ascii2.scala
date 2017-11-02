package datasets

import latis.metadata._
import latis.reader._
import latis.ops._
import latis.model._

object spark_ascii2 extends DatasetDescriptor(
  Metadata("id" -> "spark_ascii2")
)(
  Function(Metadata("id" -> "f"))(
    Text(id="myTime"),
    Tuple(
      Integer(id="myInt"),
      Real(id="myReal"),
      Text(id="myText")
    )
  )
)(
  SparkDataFrameAdapter(location = "ascii2")
)(
  //Operations
)
