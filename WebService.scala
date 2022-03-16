import org.scalatra.ScalatraServlet

class WebService extends ScalatraServlet  {
  get("/") {
    "Scalatra rules!"
    //mandi la home
  }
  get("/?matrix=par&joining=seq"){
    val c = new Controller()
    c.run(par_ma)


    send({

    })
  }
}


