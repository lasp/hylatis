package latis.server

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import latis.reader.tsml.TsmlReader2
import latis.dm._

class LatisSparkServer extends LatisServer {
  //TODO: clean up? session.close()? or only when server goes down anyway?
  
  override def init(): Unit = {
    super.init
    //TODO: do once per server, not servlet
    //TODO: get catalog and call for each dataset, do in parallel
    loadData("ascii")
  }
  
  //TODO: SparkUtils?
  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("TestSparkAdapter")
      .getOrCreate()
  }
  
  private def loadData(dsName: String): Unit = {
    val ds = TsmlReader2.fromName(dsName).getDataset()
    //TODO: get from dataset
    val vnames = Seq("myTime", "myInt", "myReal", "myText")
    val types = Seq(StringType, LongType, DoubleType, StringType)
    val fields = vnames.zip(types).map(p => StructField(p._1, p._2, nullable = true))
    val schema = StructType(fields)
    val rows = ds match {
      case Dataset(Function(it)) => it.toSeq map { sample => sample match {
        case Sample(Text(t), TupleMatch(Integer(i), Real(r), Text(s))) =>
          Row(t,i,r,s)
      }}
    }
    
    val sparkSession = getSparkSession
    val rdd = sparkSession.sparkContext.parallelize(rows)
    val df = sparkSession.createDataFrame(rdd, schema)
    df.createOrReplaceTempView(dsName)
  }
}