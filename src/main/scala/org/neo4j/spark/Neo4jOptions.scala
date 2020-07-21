package org.neo4j.spark

import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.{AccessMode, SessionConfig}
import org.neo4j.driver.Config.TrustStrategy

class Neo4jOptions(private val parameters: java.util.Map[String, String]) extends Serializable {

  import Neo4jOptions._
  import QueryType._

  private def getRequiredParameter(parameter: String): String = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    parameters.get(parameter)
  }

  private def getParameter(parameter: String, defaultValue: String = ""): String = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      return defaultValue
    }

    parameters.get(parameter).trim()
  }

  val query: Neo4jQueryOptions = (
    getParameter(QUERY.toString.toLowerCase),
    getParameter(LABELS.toString.toLowerCase),
    getParameter(RELATIONSHIP.toString.toLowerCase())
  ) match {
    case (query, "", "") => Neo4jQueryOptions(QUERY, query)
    case ("", label, "") => Neo4jQueryOptions(LABELS, label)
    case ("", "", relationship) => Neo4jQueryOptions(RELATIONSHIP, relationship)
    case _ => throw new IllegalArgumentException(
      s"You need to specify just one of these options: ${QueryType.values.toSeq.map( value => s"'${value.toString.toLowerCase()}'")
        .sorted.mkString(", ")}"
    )
  }

  val connection: Neo4jDriverOptions = Neo4jDriverOptions(
    getRequiredParameter(URL),
    getParameter(AUTH_TYPE, DEFAULT_AUTH_TYPE),
    getParameter(AUTH_BASIC_USERNAME, DEFAULT_AUTH_BASIC_USERNAME),
    getParameter(AUTH_BASIC_PASSWORD, DEFAULT_AUTH_BASIC_PASSWORD),
    getParameter(ENCRYPTION_ENABLED, DEFAULT_ENCRYPTION_ENABLED.toString).toBoolean,
    TrustStrategy.Strategy.valueOf(getParameter(ENCRYPTION_TRUST_STRATEGY, DEFAULT_ENCRYPTION_TRUST_STRATEGY.name())),
    getParameter(ENCRYPTION_CA_CERTIFICATE_PATH, DEFAULT_ENCRYPTION_CA_CERTIFICATE_PATH),
    getParameter(MAX_CONNECTION_LIFETIME_MSECS, DEFAULT_MAX_CONNECTION_LIFETIME_MSECS.toString).toInt,
    getParameter(MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS, DEFAULT_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS.toString).toInt
  )

  val session: Neo4jSessionOptions =  Neo4jSessionOptions(
    getParameter(DATABASE, DEFAULT_DATABASE),
    AccessMode.valueOf(getParameter(ACCESS_MODE, DEFAULT_ACCESS_MODE.toString).toUpperCase())
  )
}

case class Neo4jQueryOptions(queryType: QueryType.Value, value: String) extends Serializable

case class Neo4jSessionOptions(database: String, accessMode: AccessMode = AccessMode.READ) {
  def toNeo4jSession: SessionConfig = {
    val builder = SessionConfig.builder()
      .withDefaultAccessMode(accessMode)

    if(database != null && database != "") {
      builder.withDatabase(database)
    }

    builder.build()
  }

}

case class Neo4jDriverOptions(
                               url: String,
                               auth: String,
                               username: String,
                               password: String,
                               encryption: Boolean,
                               trustStrategy: TrustStrategy.Strategy,
                               certificatePath: String,
                               lifetime: Int,
                               timeout: Int
                             ) extends Serializable

object Neo4jOptions {

  // connection options
  val URL = "url"
  val AUTH_TYPE = "authentication.type"
  val AUTH_BASIC_USERNAME = "authentication.basic.username"
  val AUTH_BASIC_PASSWORD = "authentication.basic.password"
  val ENCRYPTION_ENABLED = "encryption.enabled"
  val ENCRYPTION_TRUST_STRATEGY = "encryption.trust.strategy"
  val ENCRYPTION_CA_CERTIFICATE_PATH = "encryption.ca.certificate.path"
  val MAX_CONNECTION_LIFETIME_MSECS = "connection.max.lifetime.msecs"
  val MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS = "connection.acquisition.timeout.msecs"

  // session options
  val DATABASE = "database"
  val ACCESS_MODE = "access.mode"

  // defaults
  val DEFAULT_DATABASE = ""
  val DEFAULT_ACCESS_MODE = AccessMode.WRITE
  val DEFAULT_AUTH_TYPE = "basic"
  val DEFAULT_AUTH_BASIC_USERNAME = ""
  val DEFAULT_AUTH_BASIC_PASSWORD = ""
  val DEFAULT_ENCRYPTION_ENABLED = false
  val DEFAULT_ENCRYPTION_TRUST_STRATEGY = TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
  val DEFAULT_ENCRYPTION_CA_CERTIFICATE_PATH = ""
  val DEFAULT_MAX_CONNECTION_LIFETIME_MSECS = 1000
  val DEFAULT_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS = 1000
}

object QueryType extends Enumeration {
  val QUERY, LABELS, RELATIONSHIP = Value
}