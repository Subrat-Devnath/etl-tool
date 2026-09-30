package etl.model.transform.task;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_debug")
public class Debug extends TransformTask {

    private static final long serialVersionUID = 1L;

    @Column(value = "action")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String action;

    @Override
    public String name() {
        return "debug";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        DebugAction.of(action).apply(ds);
        return ds;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

}
