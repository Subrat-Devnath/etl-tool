package etl.model.transform.task;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_struct")
public class Struct extends TransformTask {

    private static final long serialVersionUID = 1L;

    @org.springframework.data.cassandra.core.mapping.Column(value = "col")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> col;

    @org.springframework.data.cassandra.core.mapping.Column(value = "target")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String target;

    @org.springframework.data.cassandra.core.mapping.Column(value = "keep")
    @CassandraType(type = CassandraType.Name.BOOLEAN)
    public Boolean keep;

    @Override
    public String name() {
        return "struct";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        Column[] cols = col.stream().map(ds::col).toArray(Column[]::new);
        Dataset<Row> out = ds.withColumn(target, functions.struct(cols));

        if (keep == null || !keep) {
            out = out.drop(col.toArray(new String[0]));
        }

        return out;
    }

    public List<String> getCol() {
        return col;
    }

    public void setCol(List<String> col) {
        this.col = col;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Boolean getKeep() {
        return keep;
    }

    public void setKeep(Boolean keep) {
        this.keep = keep;
    }

}
