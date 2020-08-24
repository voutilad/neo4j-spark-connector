package org.neo4j.spark

import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SaveMode
import org.neo4j.driver.Config.TrustStrategy
import org.neo4j.driver._
import org.neo4j.spark.util.Neo4jUtil


class Neo4jOptions(private val parameters: java.util.Map[String, String]) extends Serializable {

  import Neo4jOptions._
  import QueryType._

  private def getRequiredParameter(parameter: String): String = {
    if (!parameters.containsKey(parameter) || parameters.get(parameter).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    parameters.get(parameter)
  }

  private def getParameter(parameter: String, defaultValue: String = ""): String = {
    if (!parameters.containsKey(parameter) || parameters.get(parameter).isEmpty) {
      return defaultValue
    }

    parameters.get(parameter).trim()
  }

  val pushdownFiltersEnabled: Boolean = getParameter(PUSHDOWN_FILTERS_ENABLED, DEFAULT_PUSHDOWN_FILTERS_ENABLED.toString).toBoolean

  val schemaMetadata = Neo4jSchemaMetadata(getParameter(SCHEMA_FLATTEN_LIMIT, DEFAULT_SCHEMA_FLATTEN_LIMIT.toString).toInt,
    SchemaStrategy.withName(getParameter(SCHEMA_STRATEGY, DEFAULT_SCHEMA_STRATEGY.toString).toUpperCase))

  val query: Neo4jQueryOptions = (
    getParameter(QUERY.toString.toLowerCase),
    getParameter(LABELS.toString.toLowerCase),
    getParameter(RELATIONSHIP.toString.toLowerCase())
  ) match {
    case (query, "", "") => Neo4jQueryOptions(QUERY, query)
    case ("", label, "") => {
      val parsed = if (label.trim.startsWith(":")) label.substring(1) else label
      Neo4jQueryOptions(LABELS, parsed)
    }
    case ("", "", relationship) => Neo4jQueryOptions(RELATIONSHIP, relationship)
    case _ => throw new IllegalArgumentException(
      s"You need to specify just one of these options: ${
        QueryType.values.toSeq.map(value => s"'${value.toString.toLowerCase()}'")
          .sorted.mkString(", ")
      }"
    )
  }

  val connection: Neo4jDriverOptions = Neo4jDriverOptions(
    getRequiredParameter(URL),
    getParameter(AUTH_TYPE, DEFAULT_AUTH_TYPE),
    getParameter(AUTH_BASIC_USERNAME, DEFAULT_EMPTY),
    getParameter(AUTH_BASIC_PASSWORD, DEFAULT_EMPTY),
    getParameter(AUTH_KERBEROS_TICKET, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_PRINCIPAL, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_CREDENTIALS, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_REALM, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_SCHEME, DEFAULT_EMPTY),
    getParameter(ENCRYPTION_ENABLED, DEFAULT_ENCRYPTION_ENABLED.toString).toBoolean,
    Option(getParameter(ENCRYPTION_TRUST_STRATEGY, null)),
    getParameter(ENCRYPTION_CA_CERTIFICATE_PATH, DEFAULT_EMPTY),
    getParameter(CONNECTION_MAX_LIFETIME_MSECS, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(CONNECTION_ACQUISITION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(CONNECTION_LIVENESS_CHECK_TIMEOUT, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(CONNECTION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt
  )

  val session: Neo4jSessionOptions = Neo4jSessionOptions(
    getParameter(DATABASE, DEFAULT_EMPTY),
    AccessMode.valueOf(getParameter(ACCESS_MODE, DEFAULT_ACCESS_MODE.toString).toUpperCase())
  )

  val nodeMetadata = initNeo4jNodeMetadata()

  private def initNeo4jNodeMetadata(nodeKeysString: String = getParameter(NODE_KEYS, ""),
                                    labelsString: String = query.value): Neo4jNodeMetadata = {
    val nodeKeys = nodeKeysString
      .split(",")
      .map(_.trim)
      .filter(!_.isEmpty)
      .map(s => {
        val keys = s.split(":")
        if (keys.length == 2) {
          (keys(0), keys(1))
        } else {
          (keys(0), keys(0))
        }
      })
      .toMap
    val labels = labelsString
      .split(":")
      .map(_.trim)
      .filter(!_.isEmpty)
    Neo4jNodeMetadata(labels, nodeKeys)
  }

  val transactionMetadata = initNeo4jTransactionMetadata()

  private def initNeo4jTransactionMetadata(): Neo4jTransactionMetadata = {
    val retries = getParameter(TRANSACTION_RETRIES, DEFAULT_TRANSACTION_RETRIES.toString).toInt
    val failOnTransactionCodes = getParameter(TRANSACTION_CODES_FAIL, DEFAULT_EMPTY)
      .split(",")
        .map(_.trim)
        .filter(!_.isEmpty)
        .toSet
    val batchSize = getParameter(BATCH_SIZE, DEFAULT_BATCH_SIZE.toString).toInt
    Neo4jTransactionMetadata(retries, failOnTransactionCodes, batchSize)
  }

  val relationshipMetadata = initNeo4jRelationshipMetadata()

  def initNeo4jRelationshipMetadata(): Neo4jRelationshipMetadata = {
    val source = initNeo4jNodeMetadata(getParameter(RELATIONSHIP_SOURCE_NODE_KEYS, ""),
      getParameter(RELATIONSHIP_SOURCE_LABELS, ""))

    val target = initNeo4jNodeMetadata(getParameter(RELATIONSHIP_TARGET_NODE_KEYS, ""),
      getParameter(RELATIONSHIP_TARGET_LABELS, ""))

    val nodeMap = getParameter(RELATIONSHIP_NODES_MAP, DEFAULT_RELATIONSHIP_NODES_MAP.toString).toBoolean

    val writeStrategy = RelationshipWriteStrategy
      .values
      .find(_.toString == getParameter(RELATIONSHIP_WRITE_STRATEGY, DEFAULT_RELATIONSHIP_WRITE_STRATEGY.toString).toUpperCase)

    if(writeStrategy.isEmpty) {
      throw new IllegalArgumentException(s"The relationship write strategy `${getParameter(RELATIONSHIP_WRITE_STRATEGY)}` is not valid, use one of ${
        RelationshipWriteStrategy.values.toSeq.sortBy(_.toString).map(value => s"'${value.toString.toLowerCase()}'")
          .sorted.mkString(", ")
      }")
    }

    val sourceSaveMode = NodeWriteMode
      .values
      .find(_.toString.toUpperCase == getParameter(RELATIONSHIP_SOURCE_WRITE_MODE, DEFAULT_RELATIONSHIP_SOURCE_WRITE_MODE.toString).toUpperCase)
    if(sourceSaveMode.isEmpty) {
      throw new IllegalArgumentException(s"The source node save mode `${getParameter(RELATIONSHIP_SOURCE_WRITE_MODE)}` is not valid, use one of ${
        NodeWriteMode.values.toSeq.map(value => s"'${value.toString.toLowerCase()}'")
          .sorted.mkString(", ")
      }")
    }

    val targetSaveMode = NodeWriteMode
      .values
      .find(_.toString.toUpperCase == getParameter(RELATIONSHIP_TARGET_WRITE_MODE, DEFAULT_RELATIONSHIP_TARGET_WRITE_MODE.toString).toUpperCase)
    if(targetSaveMode.isEmpty) {
      throw new IllegalArgumentException(s"The target node save mode `${getParameter(RELATIONSHIP_TARGET_WRITE_MODE)}` is not valid, use one of ${
        NodeWriteMode.values.toSeq.map(value => s"'${value.toString.toLowerCase()}'")
          .sorted.mkString(", ")
      }")
    }

    Neo4jRelationshipMetadata(source, target, sourceSaveMode.get, targetSaveMode.get, query.value, nodeMap, writeStrategy.get)
  }

  def initNeo4jQueryMetadata(): Neo4jQueryMetadata = Neo4jQueryMetadata(
    query.value.trim, getParameter(QUERY_COUNT, "").trim
  )

  val queryMetadata = initNeo4jQueryMetadata()

  val partitions = getParameter(PARTITIONS, DEFAULT_PARTITIONS.toString).toInt

  def validate(validationFunction: Neo4jOptions => Unit): Neo4jOptions = {
    validationFunction(this)
    this
  }
}

case class Neo4jSchemaMetadata(flattenLimit: Int, strategy: SchemaStrategy.Value)
case class Neo4jTransactionMetadata(retries: Int, failOnTransactionCodes: Set[String], batchSize: Int)

case class Neo4jNodeMetadata(labels: Seq[String], nodeKeys: Map[String, String])
case class Neo4jRelationshipMetadata(
                                      source: Neo4jNodeMetadata,
                                      target: Neo4jNodeMetadata,
                                      sourceWriteMode: NodeWriteMode.Value,
                                      targetWriteMode: NodeWriteMode.Value,
                                      relationshipType: String,
                                      nodeMap: Boolean,
                                      writeStrategy: RelationshipWriteStrategy.Value
                                    )
case class Neo4jQueryMetadata(query: String, queryCount: String)

case class Neo4jQueryOptions(queryType: QueryType.Value, value: String)

case class Neo4jSessionOptions(database: String, accessMode: AccessMode = AccessMode.READ) {
  def toNeo4jSession: SessionConfig = {
    val builder = SessionConfig.builder()
      .withDefaultAccessMode(accessMode)

    if (database != null && database != "") {
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
                               ticket: String,
                               principal: String,
                               credentials: String,
                               realm: String,
                               schema: String,
                               encryption: Boolean,
                               trustStrategy: Option[String],
                               certificatePath: String,
                               lifetime: Int,
                               acquisitionTimeout: Int,
                               livenessCheckTimeout: Int,
                               connectionTimeout: Int
                             ) extends Serializable {

  def toDriverConfig: Config = {
    val builder = Config.builder().withUserAgent(s"neo4j-spark-connector/${Neo4jUtil.connectorVersion}")

    if (lifetime > -1) builder.withMaxConnectionLifetime(lifetime, TimeUnit.MILLISECONDS)
    if (acquisitionTimeout > -1) builder.withConnectionAcquisitionTimeout(acquisitionTimeout, TimeUnit.MILLISECONDS)
    if (livenessCheckTimeout > -1) builder.withConnectionLivenessCheckTimeout(livenessCheckTimeout, TimeUnit.MILLISECONDS)
    if (connectionTimeout > -1) builder.withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
    URI.create(url).getScheme match {
      case "neo4j+s" | "neo4j+ssc" | "bolt+s" | "bolt+ssc" => Unit
      case _ => {
        if (!encryption) {
          builder.withoutEncryption()
        }
        else {
          builder.withEncryption()
        }
        trustStrategy
          .map(Config.TrustStrategy.Strategy.valueOf)
          .map {
            case TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES => TrustStrategy.trustAllCertificates()
            case TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES => TrustStrategy.trustSystemCertificates()
            case TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES => TrustStrategy.trustCustomCertificateSignedBy(new File(certificatePath))
          }.foreach(builder.withTrustStrategy)
      }
    }

    builder.build()
  }

  def toNeo4jAuth: AuthToken = {
    auth match {
      case "basic" => AuthTokens.basic(username, password)
      case "none" => AuthTokens.none()
      case "kerberos" => AuthTokens.kerberos(ticket)
      case "custom" => AuthTokens.custom(principal, credentials, realm, schema)
      case _ => throw new IllegalArgumentException(s"Authentication method '${auth}' is not supported")
    }
  }
}

object Neo4jOptions {

  // connection options
  val URL = "url"

  // auth
  val AUTH_TYPE = "authentication.type" // basic, none, kerberos, custom
  val AUTH_BASIC_USERNAME = "authentication.basic.username"
  val AUTH_BASIC_PASSWORD = "authentication.basic.password"
  val AUTH_KERBEROS_TICKET = "authentication.kerberos.ticket"
  val AUTH_CUSTOM_PRINCIPAL = "authentication.custom.principal"
  val AUTH_CUSTOM_CREDENTIALS = "authentication.custom.credentials"
  val AUTH_CUSTOM_REALM = "authentication.custom.realm"
  val AUTH_CUSTOM_SCHEME = "authentication.custom.scheme"

  // driver
  val ENCRYPTION_ENABLED = "encryption.enabled"
  val ENCRYPTION_TRUST_STRATEGY = "encryption.trust.strategy"
  val ENCRYPTION_CA_CERTIFICATE_PATH = "encryption.ca.certificate.path"
  val CONNECTION_MAX_LIFETIME_MSECS = "connection.max.lifetime.msecs"
  val CONNECTION_LIVENESS_CHECK_TIMEOUT = "connection.liveness.timeout.msec"
  val CONNECTION_ACQUISITION_TIMEOUT_MSECS = "connection.acquisition.timeout.msecs"
  val CONNECTION_TIMEOUT_MSECS = "connection.timeout.msecs"

  // session options
  val DATABASE = "database"
  val ACCESS_MODE = "access.mode"
  val WRITE_MODE = "write.mode"

  val PUSHDOWN_FILTERS_ENABLED = "pushdown.filters.enabled"

  // schema options
  val SCHEMA_STRATEGY = "schema.strategy"
  val SCHEMA_FLATTEN_LIMIT = "schema.flatten.limit"

  // partitions
  val PARTITIONS = "partitions"

  // Node Metadata
  val NODE_KEYS = "node.keys"
  val BATCH_SIZE = "batch.size"
  val SUPPORTED_SAVE_MODES = Seq(SaveMode.Overwrite, SaveMode.ErrorIfExists)

  // Relationship Metadata
  val RELATIONSHIP_SOURCE_LABELS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.${QueryType.LABELS.toString.toLowerCase}"
  val RELATIONSHIP_SOURCE_NODE_KEYS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$NODE_KEYS"
  val RELATIONSHIP_SOURCE_WRITE_MODE = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$WRITE_MODE"
  val RELATIONSHIP_TARGET_LABELS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.${QueryType.LABELS.toString.toLowerCase}"
  val RELATIONSHIP_TARGET_NODE_KEYS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$NODE_KEYS"
  val RELATIONSHIP_TARGET_WRITE_MODE = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$WRITE_MODE"
  val RELATIONSHIP_NODES_MAP = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.nodes.map"
  val RELATIONSHIP_WRITE_STRATEGY = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.write.strategy"

  // Query metadata
  val QUERY_COUNT = "query.count"

  // Transaction Metadata
  val TRANSACTION_RETRIES = "transaction.retries"
  val TRANSACTION_CODES_FAIL = "transaction.codes.fail"

  // defaults
  val DEFAULT_EMPTY = ""
  val DEFAULT_TIMEOUT: Int = -1
  val DEFAULT_ACCESS_MODE = AccessMode.READ
  val DEFAULT_AUTH_TYPE = "basic"
  val DEFAULT_ENCRYPTION_ENABLED = false
  val DEFAULT_ENCRYPTION_TRUST_STRATEGY = TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
  val DEFAULT_SCHEMA_FLATTEN_LIMIT = 10
  val DEFAULT_BATCH_SIZE = 5000
  val DEFAULT_TRANSACTION_RETRIES = 3
  val DEFAULT_RELATIONSHIP_NODES_MAP = true
  val DEFAULT_SCHEMA_STRATEGY = SchemaStrategy.SAMPLE
  val DEFAULT_RELATIONSHIP_WRITE_STRATEGY: RelationshipWriteStrategy.Value = RelationshipWriteStrategy.NATIVE
  val DEFAULT_RELATIONSHIP_SOURCE_WRITE_MODE: NodeWriteMode.Value = NodeWriteMode.Match
  val DEFAULT_RELATIONSHIP_TARGET_WRITE_MODE: NodeWriteMode.Value = NodeWriteMode.Match
  val DEFAULT_PUSHDOWN_FILTERS_ENABLED = true
  val DEFAULT_PARTITIONS = 1
}

object QueryType extends Enumeration {
  val QUERY, LABELS, RELATIONSHIP = Value
}

object RelationshipWriteStrategy extends Enumeration {
  val NATIVE, KEYS = Value
}

object NodeWriteMode extends Enumeration {
  val Overwrite, ErrorIfExists, Match = Value

  def fromSaveMode(saveMode: SaveMode): Value = {
    saveMode match {
      case SaveMode.Overwrite => Overwrite
      case SaveMode.ErrorIfExists => ErrorIfExists
      case _ => throw new IllegalArgumentException(s"SaveMode $saveMode not supported")
    }
  }
}

object SchemaStrategy extends Enumeration {
  val STRING, SAMPLE = Value
}