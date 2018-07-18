package latis.server

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler

import javax.servlet.http._

import latis.ops._
import latis.output._
import latis.input._
import latis.util.RegEx._
import java.net.URLDecoder
import latis.util.LatisProperties

class GoesServer extends HttpServlet {
  //TODO: make catalog of datasets from *completed* spark datasets

  override def init(): Unit = {
    //TODO: load all datasets in catalog, "cache" to spark
    val reader = GoesGranuleListReader() 
    val ds = reader.getDataset()
    SparkWriter().write(ds)
  }

  override def doGet(
    request: HttpServletRequest,
    response: HttpServletResponse
  ): Unit = {
    // path is datasetName.suffix
    val ss = request.getPathInfo.split('.')
    val datasetName = ss(0).drop(1) // Drop the leading "/"
    val suffix = ss(1)

    val ops: Seq[Operation] = request.getQueryString match {
      case s: String => s.split("&").map(x => URLDecoder.decode(x, "UTF-8")).map(parseOp(_))
      case _ => Seq.empty
    }

    val ds = DatasetSource.fromName(datasetName).getDataset(ops)

    val writer: Writer = suffix match {
      case "png" => ImageWriter(response.getOutputStream, "png")
      case _ => Writer(response.getOutputStream)
    }
    writer.write(ds)

    response.setStatus(HttpServletResponse.SC_OK)
    response.flushBuffer()
  }
  
  def parseOp(expression: String): Operation =  expression match {
      case SELECTION.r(name, op, value) => Select(name, op, value)
      case OPERATION.r(name, args) => (name,args) match {
        case ("rgbPivot", args) =>
          val as = args.split(",")
          val pivotVar = as.head
          val Array(r,g,b) = as.tail.map(_.toDouble)
          RGBImagePivot(pivotVar, r, g, b)
        case ("uncurry", _) => Uncurry()
        case ("read", _) => GoesImageReaderOperation()
      }
//        //for testing handling of http errors
//        case ("httpError", s: String) => throw new HTTPException(s.toInt) 
//        
//        case (_, s: String) => Operation(name, s.split(","))
//        //args will be null if there are none, e.g. first()
//        case (_, null) => Operation(name)
//      }
      case _ => throw new UnsupportedOperationException("Failed to parse expression: '" + expression + "'")
      //TODO: log and return None? probably should return error
  }

}

object GoesServer {

  def main(args: Array[String]): Unit = {
    val server = new Server(8090)

    val context = new ServletContextHandler()
    context.setContextPath("/latis-hylatis")
    val handler = context.addServlet(classOf[GoesServer], "/latis/*")
    handler.setInitOrder(1)

    server.setHandler(context)
    server.start()
    server.join()
    
    //TODO: shut down spark
  }
}
