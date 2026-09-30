package etl.model.transform.task;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_json")
public class Json extends TransformTask {

    private static final long serialVersionUID = 1L;

    @org.springframework.data.cassandra.core.mapping.Column(value = "col")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> col;

    @Override
    public String name() {
        return "json";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        Column[] cols = col.stream().map(ds::col).toArray(Column[]::new);
        Column json = functions.call_udf("col_to_json", functions.struct(cols));

        return spark.read().json(ds.select(json).as(Encoders.STRING()));
    }

    public List<String> getCol() {
        return col;
    }

    public void setCol(List<String> col) {
        this.col = col;
    }

}
