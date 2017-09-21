package latis.server

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler

import latis.reader.DatasetSource
import latis.writer.SparkDataFrameWriter
import latis.writer.Writer3

class LatisHylatisServer extends HttpServlet {

  override def init(): Unit = {
    loadData("ascii")
  }

  override def doGet(
    request: HttpServletRequest,
    response: HttpServletResponse
  ): Unit = {
    // Making the assumption that the request is just the name of the
    // dataset without any suffix or query.
    val name = {
      val path = request.getPathInfo
      // Drop the leading "/"
      path.drop(1)
    }

    val ds = DatasetSource.fromName(name).getDataset()

    val writer = {
      val os = response.getOutputStream
      Writer3(os)
    }
    writer.write(ds)

    response.setStatus(HttpServletResponse.SC_OK)
    response.flushBuffer()
  }

  private def loadData(name: String): Unit = {
    val ds = DatasetSource.fromName(name).getDataset()
    SparkDataFrameWriter.write(ds)
  }
}

object LatisHylatisServer {

  def main(args: Array[String]): Unit = {
    val server = new Server(8090)

    val context = new ServletContextHandler()
    context.setContextPath("/latis-hylatis")
    context.addServlet(classOf[LatisHylatisServer], "/latis/*")

    server.setHandler(context)
    server.start()
    server.join()
  }
}
