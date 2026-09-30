package etl.model.transform.task;

import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_filter")
public class Filter extends TransformTask {

    private static final long serialVersionUID = 1L;

    @org.springframework.data.cassandra.core.mapping.Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @org.springframework.data.cassandra.core.mapping.Column(value = "op")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String op;

    @org.springframework.data.cassandra.core.mapping.Column(value = "value")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String value;

    @org.springframework.data.cassandra.core.mapping.Column(value = "ref")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String ref;

    @org.springframework.data.cassandra.core.mapping.Column(value = "inverse")
    @CassandraType(type = CassandraType.Name.BOOLEAN)
    public Boolean inverse;

    @Override
    public String name() {
        return "filter";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        Column filter = Operator.of(op).apply(ds.col(col), ds, this);

        if (inverse != null && inverse) {
            filter = functions.not(filter);
        }

        return ds.filter(filter);
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public Boolean getInverse() {
        return inverse;
    }

    public void setInverse(Boolean inverse) {
        this.inverse = inverse;
    }

}
