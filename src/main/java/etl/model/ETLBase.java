package etl.model;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;

import sidecar.Logger;

public abstract class ETLBase implements Logger {

    @Column(value = "source")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String source;

    @Column(value = "target")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String target;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public abstract void execute(Job job, SparkSession spark, Map<String, Dataset<Row>> dataset);

}
