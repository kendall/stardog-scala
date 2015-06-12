package starla

import com.sun.xml.internal.ws.policy.jaxws.SafePolicyReader
import dispatch._, Defaults._
import starla.StardogOpType.QueryPlan
import util._
import scala.concurrent.Future
import codecs._

sealed trait StardogType
object StardogType {
  case object TxId extends StardogType //needs state, really, to hold the id? then not really a type...
  val values: Set[StardogType] = Values
}
import StardogType._

case class TxId[TxId](txId: String) {}

sealed trait SelectQueryMimeType
object SelectQueryMimeType {
  case object SparqlXml extends SelectQueryMimeType {
    val MIME = "application/sparql-results+xml"
  }
  case object SparqlJson extends SelectQueryMimeType {
    val MIME = "application/sparql-results+json"
  }
  case object SparqlBool extends SelectQueryMimeType {
    val MIME = "text/boolean"
  }
  case object SparqlBinary extends SelectQueryMimeType {
    val MIME = "application/x-binary-ref-results-table"
  }
  val values: Set[SelectQueryMimeType] = Values
}
import SelectQueryMimeType._

sealed trait StardogOpType
object StardogOpType {
  case object QueryPlan extends StardogOpType {
    val MIME = "text/plain"
  }
  case object DbSize extends StardogOpType {
    val MIME = "text/plain"
  }
  case object Db extends StardogOpType {
    val MIME = "*/*"
  }
  case object BeginTx extends StardogOpType {
    val MIME = "text/plain"
  }
  case object CommitTx extends StardogOpType {
    val MIME = "text/plain"
  }
  case object RollbackTx extends StardogOpType
  case object Tx extends StardogOpType
  val values: Set[StardogOpType] = Values
}
import StardogOpType._

sealed trait SparqlUpdateType
object SparqlUpdateType {
  case object Update extends SparqlUpdateType {
    val MIME = "application/sparql-update"
  }
  case object FormUrlEncoded extends SparqlUpdateType {
    val MIME = "application/x-www-form-urlencoded"
  }
  val values: Set[SparqlUpdateType] = Values
}
import SparqlUpdateType._

sealed trait StardogHttpMethodType
object StardogHttpMethodType {
  case object GET  extends StardogHttpMethodType
  case object POST extends StardogHttpMethodType
  val values: Set[StardogHttpMethodType] = Values
}
import StardogHttpMethodType._

sealed trait StardogErrorCode
object StardogErrorCode {
  case object AuthenticationError extends StardogErrorCode
  case object AuthorizationError extends StardogErrorCode
  case object QueryEvalError extends StardogErrorCode
  case object QueryParseError extends StardogErrorCode
  case object UnknownQuery extends StardogErrorCode
  case object UnknownTx extends StardogErrorCode
  case object UnknownDb extends StardogErrorCode
  case object DbExists extends StardogErrorCode
  case object BadDbName extends StardogErrorCode
  case object SecurityResourceExists extends StardogErrorCode
  case object BadConnParam extends StardogErrorCode
  case object BadDbState extends StardogErrorCode
  case object ResourceInUse extends StardogErrorCode
  case object UnknownResource extends StardogErrorCode
  case object BadOperation extends StardogErrorCode
  case object BadPassword extends StardogErrorCode

  val values: Set[StardogErrorCode] = Values

  def errCodeToType(e: Int): Option[StardogErrorCode] = Map[Int,StardogErrorCode](
    0 -> AuthenticationError,
    1 -> AuthorizationError,
    2 -> QueryEvalError,
    3 -> QueryParseError,
    4 -> UnknownQuery,
    5 -> UnknownTx,
    6 -> UnknownDb,
    7 -> DbExists,
    8 -> BadDbName,
    9 -> SecurityResourceExists,
   10 -> BadConnParam,
   11 -> BadDbState,
   12 -> ResourceInUse,
   13 -> UnknownResource,
   14 -> BadOperation,
   15 -> BadPassword
  ) get e
}
import StardogErrorCode._

sealed trait StardogResponseCode
object StardogResponseCode {
  case object Success extends StardogResponseCode
  case object SuccessWait extends StardogResponseCode
  case object ParseOrTxFail extends StardogResponseCode
  case object Unauthorized extends StardogResponseCode
  case object BadCredentials extends StardogResponseCode
  case object NoSuchResource extends StardogResponseCode
  case object SystemConflict extends StardogResponseCode
  case object UnknownFail extends StardogResponseCode

  val values: Set[StardogResponseCode] = Values

  def isSuccessCode(c: StardogResponseCode): Boolean = c match {
    case Success | SuccessWait => true
    case _ => false
  }
  def isFailCode(c: StardogResponseCode): Boolean = !isSuccessCode(c)
  
  def httpResponseToType(r: Int): Option[StardogResponseCode] = Map[Int,StardogResponseCode](
    200 -> Success,
    202 -> SuccessWait,
    400 -> ParseOrTxFail,
    401 -> Unauthorized,
    403 -> BadCredentials,
    404 -> UnknownFail,
    409 -> SystemConflict,
    500 -> UnknownFail
  ) get r
}
import StardogResponseCode._

case class Success[Success](){}
case class UnknownFail[UnknownFail](){}

abstract case class HTTP(
                          host:                 String,
                          port:                 Int                   = 5820,
                          https:                Boolean               = false,
                          private var username: String                = "admin",
                          private var password: String                = "admin",
                          var dbName:           String,
                          var reasoning:        Boolean               = false,
                          var queryMethod:      StardogHttpMethodType = GET,
                          var accept:           Option[String]
                          ) {
  val VERSION                  = "0.0.1"
  val MIN_STARDOG_VERSION      = "3.1" //do a check on the server to see what it is...
  //this is bullshit... TODO: fixme
  //we can delete it. it doesn't add anything to have a single base/default type... there's no such thing, really, even in the
  //most common case (select)
  val SPARQL_MIME_TYPE: String = "application/sparql-results+json"
  val cred                     = Map('username -> username, 'password -> password)
  val isReasoningEnabled       = reasoning

  private def path(t: StardogOpType): String = Map[StardogOpType,String](
    QueryPlan -> "explain",
    DbSize -> "size",
    //I don't know if these are correct:
    BeginTx -> "begin",
    CommitTx -> "commit",
    RollbackTx -> "rollback",
    Tx -> "transaction"
  ) get t head //do this anywhere we have a closed system...but not where we're using input from Stardog on other end

  private def baseRequestBuilder = {
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

  //domain specific codecs have to live in bofe.models.Life.scala

  def getProperty(uri: String, property: String): Future[String] = query(s"select ?val where { $uri $property ?val}")

  def getDB: Future[String] = {
    val reqb = baseRequestBuilder <:< Map("Accept" -> StardogOpType.Db.MIME)
    Http(reqb.GET OK as.String)
  }

  def getDBSize: Future[Int] = {
    val reqb = baseRequestBuilder / path(DbSize) <:< Map("Accept" -> StardogOpType.DbSize.MIME)
    val size = Http(reqb.GET OK as.String)
    for (s <- size) yield s.toInt
  }

  def getQueryPlan: Future[String] = {
    val reqb = baseRequestBuilder / path(QueryPlan) <:< Map("Accept" -> StardogOpType.QueryPlan.MIME)
    //we need to test soemthing to see if we should be POST'ing
    Http(reqb.GET OK as.String)
  }

  /*

  which codec we use coming back depends on at least two factors:

  * which query form
  * which mime type of that query form's return type
  * so, some kind of 2-tuple map that returns codec? ...maybe

  we also have the completely separate issue that we want to return streams...future streams?
  this may not be possible in the HTTP driver
  certainly possible in the native driver...use play iteratee thing?

   */

  def ask       (q: String): Future[Boolean]
  def select    (q: String): Future[String]//some kind of result set from json
  //make sure the mime type is correct...
  //can be a post or a get...
  def construct (q: String): Future[String]//wrong...
  def describe  (q: String): Future[String]//wrong, too
  //set the right mime type, which depends on the http method (?)
  def update    (q: String): Future[Boolean]

  /*
  we can't compile without these type parameters; I suppose they're because we gave some state to these case objects?
   */


  def beginTx: Future[TxId[String]] /* = {}
    val reqb = baseRequestBuilder / path(Tx) / path(BeginTx) <:< Map("Accept" -> StardogOpType.BeginTx.MIME)
  }
  */
  def commitTx(txId: TxId[String]): Either[Success[String],UnknownFail[String]] /* = {
    val reqb = baseRequestBuilder / path(Tx) / path(CommitTx) <:< Map("Accept" -> StardogOpType.CommitTx.MIME)
  }
  */
  def rollbackTx(txId: TxId[String]): Either[Success[String],UnknownFail[String]]

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

