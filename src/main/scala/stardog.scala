package stardog

import dispatch._, Defaults._
import shapeless._
import stardog.StardogHttpMethodType._

import scala.concurrent.Future

sealed trait StardogHttpMethodType
object StardogHttpMethodType {
  case object GET  extends StardogHttpMethodType
  case object POST extends StardogHttpMethodType

  val values: Set[StardogHttpMethodType] = Values
}

abstract case class HTTP(host: String,
                port: Int = 5820,
                https: Boolean = false,
                private var username: String = "admin",
                private var password: String = "admin",
                var dbName: String,
                var reasoning: Boolean = false,
                var queryMethod: StardogHttpMethodType = GET,
                var accept: Option[String]) {
  val VERSION = "0.0.1"
  val STARDOG_VERSION = "3.1"
  val SPARQL_MIME_TYPE: String = "application/sparql-results+json"
  val cred = Map('username -> username, 'password -> password)
  val isReasoningEnabled = reasoning

  def baseRequestBuilder = {
    val myHost = dispatch.host(host, port)
    if (https) myHost.secure
    myHost.as_!(cred.get('username).head, cred.get('password).head)
    val myEndpoint = myHost / dbName
    val myRequestBuilder = accept match {
      case None => myEndpoint <:< Map("Accept" -> SPARQL_MIME_TYPE)
      case _ => myEndpoint <:< Map("Accept" -> accept.head)
    }
    myRequestBuilder
  }

  //has a argonaut json thingie to return future[json] or even better: future[resultSet] etc

  //generic sparql codecs can live here...

  //domain specific codecs have to live in bofe.models.Life.scala

  def getProperty(uri: String, property: String): Future[String] = {
    query(s"select ?val where { $uri $property ?val}")
  }

  def getDB: Future[String] = {
    val reqb = baseRequestBuilder <:< Map("Accept" -> "*/*")
    Http(reqb.GET OK as.String)
  }

  def getDBSize: Future[Int] = {
    val reqb = baseRequestBuilder <:< Map("Accept" -> "text/plain")
    val size = Http(reqb.GET OK as.String)
    for (s <- size) yield s.toInt
  }

  def ask(q: String): Future[Boolean]
  def select(q: String): Future[Map]//some kind of result set from json
  def construct(q: String): Future[String]//wrong...
  def describe(q: String): Future[Map]//wrong, too
  def update(q: String): Future[Boolean]

  def query(q: String): Future[String] = {
    //add baseURI? not doing limit or offset...
    val reqb = baseRequestBuilder
    queryMethod match {
      case POST =>
        reqb.POST
        reqb << Map("query" -> q)
        if (isReasoningEnabled) reqb << Map("reasoning" -> "true")
        //set teh content type
      case GET =>
        reqb.GET
        if (isReasoningEnabled) reqb <<? Map("reasoning" -> "")
        reqb <<? Map("query" -> q)
    }
    Http(reqb OK as.String)
  }
}

object Values {
  implicit def conv[T](self: this.type)(implicit v: MkValues[T]): Set[T] = Values[T]

  def apply[T](implicit v: MkValues[T]): Set[T] = v.values.toSet

  trait MkValues[T] {
    def values: List[T]
  }

  object MkValues {
    implicit def values[T, Repr <: Coproduct]
    (implicit gen: Generic.Aux[T, Repr], v: Aux[T, Repr]): MkValues[T] =
      new MkValues[T] {
        def values = v.values
      }

    trait Aux[T, Repr] {
      def values: List[T]
    }

    object Aux {
      implicit def cnilAux[A]: Aux[A, CNil] =
        new Aux[A, CNil] {
          def values = Nil
        }

      implicit def cconsAux[T, L <: T, R <: Coproduct]
      (implicit l: Witness.Aux[L], r: Aux[T, R]): Aux[T, L :+: R] =
        new Aux[T, L :+: R] {
          def values = l.value :: r.values
        }
    }

  }
}
