package latis.server

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletContextHandler

import javax.servlet.http._

import latis.ops._
import latis.output._
import latis.input._
import latis.util.RegEx._
import java.net.URLDecoder
import latis.util.LatisProperties
import latis.model._
import latis.metadata._
import latis.util.StreamUtils._
import latis.util.SparkUtils._
import latis.util.SparkUtils
import latis.data.RddFunction
import latis.util.CacheManager

class HylatisServer extends HttpServlet {
  //TODO: make catalog of datasets from *completed* spark datasets

  // http://localhost:8090/latis-hylatis/dap/hysics.png?rgbPivot(wavelength, 630.87, 531.86, 463.79)
  
  override def init(): Unit = {
    //TODO: load all datasets in catalog
    
    //This will load the hysics cube: (iy, ix, w) -> f
    //  and restructure it in a RddFunction with the latis dataset
    //  cached as "hysics"
    //TODO: lazy? need to force?
    HysicsReader().getDataset
    GoesReader().getDataset
  }

  override def doGet(
    request: HttpServletRequest,
    response: HttpServletResponse
  ): Unit = {
    // path is datasetName.suffix
    val ss = request.getPathInfo.split('.')
    val datasetName = ss(0).drop(1) // Drop the leading "/"
    val suffix = ss(1)

    val ops: Seq[UnaryOperation] = request.getQueryString match {
      case s: String => s.split("&").map(x => URLDecoder.decode(x, "UTF-8")).map(parseOp(_))
      case _ => Seq.empty
    }

     //DatasetSource.fromName(datasetName).getDataset(ops)
val ds0 =  CacheManager.getDataset(datasetName).get //cached during init
val ds  = ops.foldLeft(ds0)((ds, op) => op(ds))

    val writer: Writer = suffix match {
      case "png" => ImageWriter(response.getOutputStream, "png")
      case _ => new Writer(response.getOutputStream)
    }
    writer.write(ds)

    response.setStatus(HttpServletResponse.SC_OK)
    response.flushBuffer()
  }
  
  def parseOp(expression: String): UnaryOperation =  expression match {
    //TODO: use parser combinator
      //case PROJECTION.r(name) => Projection(name)
      case SELECTION.r(name, op, value) => Selection(name, op, value)
      case OPERATION.r(name, args) => (name,args) match {
        case ("rgbPivot", args) =>
          val as = args.split(",")
          val pivotVar = as.head
          val Array(r,g,b) = as.tail.map(_.toDouble)
          RGBImagePivot(pivotVar, r, g, b)
        case ("uncurry", _) => Uncurry()
        case ("bbox", args) => args.split(",") match {
          case Array(x1, y1, x2, y2, n) => 
            BoundingBoxEvaluation(x1.toDouble, 
                                  y1.toDouble,
                                  x2.toDouble,
                                  y2.toDouble,
                                  n.toInt)
          case _ => throw new UnsupportedOperationException("usage: bbox(x1,y1,x2,y2,n)")
        }
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

object HylatisServer {

  def main(args: Array[String]): Unit = {
    val server = new Server(8090)

    val context = new ServletContextHandler()
    context.setContextPath("/latis-hylatis")

    context.setResourceBase(getClass.getResource("/webapp").toString)
    context.addServlet(classOf[DefaultServlet], "/")

    val handler = context.addServlet(classOf[HylatisServer], "/dap/*")
    handler.setInitOrder(1)

    server.setHandler(context)
    server.start()
    server.join()
    
    //TODO: shut down spark
  }
}
