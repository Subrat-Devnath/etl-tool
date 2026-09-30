package etl.model.transform.task;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_cast")
public class Cast extends TransformTask {

    private static final long serialVersionUID = 1L;

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @Column(value = "format")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String format;

    @Override
    public String name() {
        return "cast";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        return ds.withColumn(col, functions.call_udf(format, ds.col(col)));
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

}
