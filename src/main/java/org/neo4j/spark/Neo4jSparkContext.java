package org.neo4j.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.neo4j.driver.v1.Type;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.Map;

/**
 * @author mh
 * @since 01.03.16
 */
public class Neo4jSparkContext {

    private final SparkContext sc;
    private final SQLContext sqlContext;

    protected Neo4jSparkContext(SparkContext sc) {
        this.sc = sc;
        sqlContext = new SQLContext(sc);
    }

    public static Neo4jSparkContext neo4jContext(SparkContext sc) {
        return new Neo4jSparkContext(sc);
    }

    public static Neo4jSparkContext neo4jContext(JavaSparkContext sc) {
        return new Neo4jSparkContext(sc.sc());
    }

    public JavaRDD<Map<String,Object>> query(final String query, final Map<String,Object> parameters) {
        return CypherRDD.apply(sc, query, parameters).toJavaRDD();
    }
    public JavaRDD<Seq<Tuple2<String, Object>>> queryTuple(final String query, final Map<String,Object> parameters) {
        return CypherTupleRDD.apply(sc, query, parameters).toJavaRDD();
    }
    public JavaRDD<Row> queryRow(final String query, final Map<String,Object> parameters) {
        return CypherRowRDD.apply(sc, query, parameters).toJavaRDD();
    }
    public DataFrame queryDF(final String query, final Map<String,Object> parameters, String...resultSchema) {
        if (resultSchema.length %2 != 0) throw new RuntimeException("Schema information has to be supplied as pairs of columnName,cypherTypeName (INTEGER,FLOAT,BOOLEAN,STRING,NULL)");
        Tuple2[] schema = new Tuple2[resultSchema.length / 2];
        for (int i = 0; i < schema.length; i++) {
            schema[i] = Tuple2.apply(resultSchema[i*2],resultSchema[i*2+1].toUpperCase());
        }
        return CypherDataFrame.apply(sqlContext, query,parameters, schema);
    }
    public DataFrame queryDF(final String query, final Map<String,Object> parameters) {
        return CypherDataFrame.apply(sqlContext, query,parameters);
    }

}
