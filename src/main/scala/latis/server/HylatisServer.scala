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
import latis.model._
import latis.metadata._
import latis.util.StreamUtils._
import latis.util.SparkUtils._
import latis.util.SparkUtils
import latis.data.RddFunction
import latis.util.CacheManager
import cats.effect.IO
import fs2.text

class HylatisServer extends HttpServlet {
  //TODO: make catalog of datasets from *completed* spark datasets

  // http://localhost:8090/latis-hylatis/dap/hysics.png?rgbPivot(wavelength, 630.87, 531.86, 463.79)
  
  override def init(): Unit = {
    //TODO: load all datasets in catalog
    
    //This will load the hysics cube: (iy, ix, w) -> f
    //  and restructure it in a RddFunction with the latis dataset
    //  cached as "hysics"
    //TODO: lazy? need to force?
    //TODO: define HysicsDataset instead of going through reader?
    HysicsReader().getDataset
    GoesReader().getDataset
    ModisReader().getDataset
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


    //Dataset.fromName(datasetName)
    //need to make sure CacheManager tries first
    val ds0 = CacheManager.getDataset(datasetName).getOrElse(Dataset.fromName(datasetName))
    // Apply operations
    val ds  = ops.foldLeft(ds0)((ds, op) => op(ds))

    suffix match {
      case "png" => ImageWriter(response.getOutputStream, "png").write(ds)
      case _ => //TODO: fix writing to output stream
        val writer = OutputStreamWriter.unsafeFromOutputStream[IO](response.getOutputStream)
        TextEncoder.encode(ds)
                   .through(text.utf8Encode)
                   .through(writer.write)
                   .compile.drain.unsafeRunSync()
    }

    response.setStatus(HttpServletResponse.SC_OK)
    response.flushBuffer()
  }
  
  def parseOp(expression: String): UnaryOperation =  expression match {
    //TODO: use parser combinator
      //case PROJECTION.r(name) => Projection(name)
      case SELECTION.r(name, op, value) => Selection(name, op, value)
      case OPERATION.r(name, args) => (name,args) match {
        case ("rgbImagePivot", args) =>
          val as = args.split(",")
          as.length match {
            case 3 =>
              val Array(r,g,b) = as.map(_.toDouble)
              RGBImagePivot("wavelength", r, g, b)
            case _ =>
              throw new UnsupportedOperationException("usage: rgbImagePivot(Wr,Wg,Wb)")
          }
        case ("uncurry", _) => Uncurry()
        case ("geoGridResample", args) => args.split(",") match {
          case Array(x1, y1, x2, y2, n) => 
            GeoGridImageResampling(x1.toDouble, 
                               y1.toDouble,
                               x2.toDouble,
                               y2.toDouble,
                               n.toInt)
          case _ => throw new UnsupportedOperationException("usage: geoGridResample(x1,y1,x2,y2,n)")
        }
        case ("gbox", args) => args.split(",") match {
          case Array(x1, y1, x2, y2) => 
            GeoBoundingBox(x1.toDouble, 
                           y1.toDouble,
                           x2.toDouble,
                           y2.toDouble)
          case _ => throw new UnsupportedOperationException("usage: gbox(lon1,lat1,lon2,lat2)")
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
