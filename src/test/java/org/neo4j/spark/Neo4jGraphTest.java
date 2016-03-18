package org.neo4j.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import scala.collection.Seq;
import scala.collection.Seq$;

/**
 * @author mh
 * @since 02.03.16
 */

public class Neo4jGraphTest {

    public static final String FIXTURE = "CREATE (:A)-[:REL]->(:B)";

    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static Neo4jSparkContext csc;
    private static ServerControls server;

    @BeforeClass
    public static void setUp() throws Exception {
        server = TestServerBuilders.newInProcessBuilder()
                .withConfig(GraphDatabaseSettings.boltConnector("0").enabled, "TRUE" )
                .withConfig(GraphDatabaseSettings.boltConnector("0").encryption_level, "OPTIONAL" )
                .withFixture(FIXTURE)
                .newServer();
        conf = new SparkConf()
                .setAppName("neoTest")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts","true")
                .set("neo4j.bolt.url", server.boltURI().toString());
        sc = new JavaSparkContext(conf);
        csc = Neo4jSparkContext.neo4jContext(sc);
    }

    @AfterClass
    public static void tearDown() {
        server.close();
    }

    @Test public void runMatrixQuery() {
        Graph graph = Neo4jGraph.loadGraph(sc.sc(), "A", Seq$.MODULE$.empty() , "B");
        assertEquals(2,graph.vertices().count());
        assertEquals(1,graph.edges().count());
    }

}
