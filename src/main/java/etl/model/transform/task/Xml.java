package etl.model.transform.task;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.databricks.spark.xml.XmlReader;

import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_xml")
public class Xml extends TransformTask {

    private static final long serialVersionUID = 1L;

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @Override
    public String name() {
        return "xml";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        JavaRDD<String> xml = ds.select(col).javaRDD().map(row -> row.mkString(""));
        return new XmlReader().xmlRdd(spark, xml.rdd());
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

}
