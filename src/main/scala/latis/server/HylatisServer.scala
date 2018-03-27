package latis.server

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import latis.input.DatasetSource
import latis.ops.Operation
import latis.writer.ImageWriter
import latis.writer.Writer

class HylatisServer extends HttpServlet {

//  override def init(): Unit = {
//    //loadData("ascii2")
//    
//    //load sample Hysics data cube
//    //TODO: need to stream data into spark, union DataFrames
//    val reader = HysicsReader()
//    val ds = reader.getDataset()
//    SparkDataFrameWriter.write(ds)
//  }

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

    val ops = parseOps(request.getQueryString)
    val ds = DatasetSource.fromName(datasetName).getDataset(ops)

    val writer: Writer = {
      val os = response.getOutputStream
      //TODO: get writer based on suffix or accepts header
      //Writer(os)
      ImageWriter(os, "png")
    }
    writer.write(ds)

    response.setStatus(HttpServletResponse.SC_OK)
    response.flushBuffer()
  }
  
  def parseOps(expression: String): Seq[Operation] = {
//    val NUM = """\d+"""
//    val pattern = s"getImage\\(($NUM),($NUM),($NUM),($NUM)\\)"
//    expression.split("&").map {
//      case pattern.r(x1,x2,y1,y2) => HysicsImageOp(x1.toInt,x2.toInt,y1.toInt,y2.toInt)
//    }
    ???
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
