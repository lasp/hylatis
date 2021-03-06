package latis.server

import java.net.URLDecoder

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletContextHandler

import cats.effect.IO
import fs2.text
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import latis.data.BinSet2D
import latis.dataset.Dataset
import latis.ops._
import latis.output.ImageWriter
import latis.output.OutputStreamWriter
import latis.output.TextEncoder
import latis.util.RegEx.OPERATION
import latis.util.RegEx.SELECTION
import latis.util.StreamUtils.contextShift

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
    //HysicsReader().getDataset
    //GoesReader().getDataset
    //ModisReader().getDataset //http://localhost:8090/latis-hylatis/dap/modis.png?geoGridResample(-110,10,-80,35,75000)&rgbImagePivot(1.0,5.0,4.0)
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


    val ds0 = Dataset.fromName(datasetName)
    // Apply operations
    val ds  = ops.foldLeft(ds0)((ds, op) => ds.withOperation(op))

    suffix match {
      case "png" => ImageWriter(response.getOutputStream, "png").write(ds)
      case _ => //TODO: fix writing to output stream
        val writer = OutputStreamWriter.unsafeFromOutputStream[IO](response.getOutputStream)
        new TextEncoder().encode(ds)
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
              RGBImagePivot(r, g, b)
            case _ =>
              throw new UnsupportedOperationException("usage: rgbImagePivot(Wr,Wg,Wb)")
          }
        case ("uncurry", _) => Uncurry()
//        case ("geoGridResample", args) => args.split(",") match {
//          case Array(x1, y1, x2, y2, n) =>
//            val domainSet = BinSet2D.fromExtents((x1.toDouble, y1.toDouble), (x2.toDouble, y2.toDouble), n.toInt)
//            Resample(domainSet)
////            GeoGridImageResampling(x1.toDouble,
////                               y1.toDouble,
////                               x2.toDouble,
////                               y2.toDouble,
////                               n.toInt)
//          case _ => throw new UnsupportedOperationException("usage: geoGridResample(x1,y1,x2,y2,n)")
//        }
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
