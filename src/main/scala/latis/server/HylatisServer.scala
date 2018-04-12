package latis.server

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler

import javax.servlet.http._

import latis.ops._
import latis.output._
import latis.input._
import latis.util.RegEx._
import java.net.URLDecoder

class HylatisServer extends HttpServlet {
  //TODO: make catalog of datasets from *completed* spark datasets

  override def init(): Unit = {
    //loadData("ascii2")
    
    //load sample Hysics data cube
    //TODO: need to stream data into spark, union DataFrames
    val reader = HysicsLocalReader()
    val ds = reader.getDataset()
    SparkDataFrameWriter.write(ds)
  }

  override def doGet(
    request: HttpServletRequest,
    response: HttpServletResponse
  ): Unit = {
    // Making the assumption that the request is just the name of the
    // dataset without any suffix or query.
    val datasetName = {
      val path = request.getPathInfo
      // Drop the leading "/"
      path.drop(1)
    }

    val ops: Seq[Operation] = request.getQueryString match {
      case s: String => s.split("&").map(x => URLDecoder.decode(x, "UTF-8")).map(parseOp(_))
      case _ => Seq.empty
    }

    val ds = DatasetSource.fromName(datasetName).getDataset(ops)

    val writer: Writer = {
      val os = response.getOutputStream
      //TODO: get writer based on suffix or accepts header
      Writer(os)
      //ImageWriter(os, "png")
    }
    writer.write(ds)

    response.setStatus(HttpServletResponse.SC_OK)
    response.flushBuffer()
  }
  
  def parseOp(expression: String): Operation =  expression match {
    //TODO: use parser combinator
      //case PROJECTION.r(name) => Projection(name)
      case SELECTION.r(name, op, value) => Select(name, op, value)
//      case OPERATION.r(name, args) => (name,args) match {
//        //for testing handling of http errors
//        case ("httpError", s: String) => throw new HTTPException(s.toInt) 
//        
//        case (_, s: String) => Operation(name, s.split(","))
//        //args will be null if there are none, e.g. first()
//        case (_, null) => Operation(name)
//      }
      case _ => throw new UnsupportedOperationException("Failed to parse expression: '" + expression + "'")
      //TODO: log and return None? probably should return error
    
//    val NUM = """\d+"""
//    val pattern = s"getImage\\(($NUM),($NUM),($NUM),($NUM)\\)"
//    expression.split("&").map {
//      case pattern.r(x1,x2,y1,y2) => HysicsImageOp(x1.toInt,x2.toInt,y1.toInt,y2.toInt)
//    }
    
  }

  private def loadData(name: String): Unit = {
//    val ds = DatasetSource.fromName(name).getDataset()
//    //val ds = datasets.ascii.getDataset()
//    SparkDataFrameWriter.write(ds)
  }
}

object HylatisServer {

  def main(args: Array[String]): Unit = {
    val server = new Server(8090)

    val context = new ServletContextHandler()
    context.setContextPath("/latis-hylatis")
    val handler = context.addServlet(classOf[HylatisServer], "/latis/*")
    handler.setInitOrder(0)

    server.setHandler(context)
    server.start()
    server.join()
  }
}
