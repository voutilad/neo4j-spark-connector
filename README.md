# Neo4j Connector to Apache Spark based on Neo4j 3.0's Bolt protocol

These are the beginnings of a Connector from Neo4j to Apache Spark 2.4 using the new binary protocol for Neo4j, Bolt.

Find [more information](http://neo4j.com/docs/developer-manual/current/#driver-manual-index) about the Bolt protocol, available drivers and documentation.

Please note that I still know very little about Apache Spark and might have done really dumb things.
Please let me know by [creating an issue](https://github.com/neo4j-contrib/neo4j-spark-connector/issues) or even better [submitting a pull request](https://github.com/neo4j-contrib/neo4j-spark-connector/pulls) to this repo.

## License

This neo4j-spark-connector is Apache 2 Licensed

## Building


Build `target/neo4j-spark-connector_2.12-full-4.0.0.jar` for Scala 2.11

    mvn clean package

## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars neo4j-spark-connector_2.11-full-4.0.0.jar`

`$SPARK_HOME/bin/spark-shell --packages neo4j-contrib:neo4j-spark-connector:4.0.0`

**sbt**

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package), in your sbt build file, add:

```scala spDependencies += "neo4j-contrib/neo4j-spark-connector:4.0.0"```

Otherwise,

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "4.0.0"
```  

**maven**  
In your pom.xml, add:   

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>neo4j-contrib</groupId>
    <artifactId>neo4j-spark-connector</artifactId>
    <version>4.0.0</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```
