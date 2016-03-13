package org.neo4j.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 02.03.16
 */

public class Neo4jRDDTest {

    public static final String QUERY1 = "MATCH (m:Movie {title:{title}}) RETURN m.released as released";
    public static final String QUERY = "MATCH (m:Movie {title:{title}}) RETURN m.released as released, m.tagline as tagline";
    public static final Map<String, Object> PARAMS = Collections.<String, Object>singletonMap("title", "The Matrix");
    public static final String FIXTURE = "CREATE (:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})";

    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static Neo4jSparkContext csc;
    private static ServerControls server;

    @BeforeClass
    public static void setUp() throws Exception {
        server = TestServerBuilders.newInProcessBuilder()
                .withFixture(FIXTURE)
                .newServer();
        conf = new SparkConf()
                .setAppName("neoTest")
                .setMaster("local[*]")
                .set("neo4j.bolt.url", server.boltURI().toString());
        sc = new JavaSparkContext(conf);
        csc = Neo4jSparkContext.neo4jContext(sc);
    }

    @AfterClass
    public static void tearDown() {
        server.close();
    }

    @Test
    public void runMatrixQuery() {
        List<Map<String, Object>> found = csc.query(QUERY, PARAMS).collect();

        assertEquals(1, found.size());
        assertEquals(1999L, found.get(0).get("released"));
        assertEquals("Welcome to the Real World", found.get(0).get("tagline"));
    }

    @Test
    public void runMatrixQueryTuple() {

        List<Seq<Tuple2<String, Object>>> found = csc.queryTuple(QUERY, PARAMS).collect();

        assertEquals(1, found.size());
        Iterator<Tuple2<String, Object>> row = found.get(0).iterator();

        Tuple2<String, Object> first = row.next();
        assertEquals("released", first._1);
        assertEquals(1999L, first._2);
        Tuple2<String, Object> next = row.next();
        assertEquals("tagline", next._1);
        assertEquals("Welcome to the Real World", next._2);
        assertEquals(false, row.hasNext());
    }

    @Test
    public void runMatrixQueryRow() {
        List<Row> found = csc.queryRow(QUERY, PARAMS).collect();
        assertEquals(1, found.size());
        Row row = found.get(0);

        assertEquals(2, row.size());
        assertEquals(1999L, row.getLong(0));
        assertEquals("Welcome to the Real World", row.getString(1));
    }
    @Test
    public void runMatrixQueryRow1() {
        List<Row> found = csc.queryRow(QUERY1, PARAMS).collect();
        assertEquals(1, found.size());
        Row row = found.get(0);

        assertEquals(1, row.size());
        assertEquals(1999L, row.getLong(0));
    }
}
