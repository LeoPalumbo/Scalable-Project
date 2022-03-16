import org.eclipse.jetty.server.Server

object Main extends App{
        val server:Server=WebServiceBuilder.buildWebService(8080,classOf[WebService])
        server.start()
        }
