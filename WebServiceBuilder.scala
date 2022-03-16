import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext

import javax.servlet.Servlet

object WebServiceBuilder{
        def buildWebService(port:Integer,webServiceClass:Class[_<:Servlet]):Server={
        val server=new Server(port)
        val context=new WebAppContext()
        context.setContextPath("/")
        context.setResourceBase("/tmp")
        context.addServlet(webServiceClass,"/*")
        server.setHandler(context)
        server
        }
}
