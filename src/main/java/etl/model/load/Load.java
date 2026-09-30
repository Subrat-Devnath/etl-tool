package etl.model.load;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.ETLBase;
import etl.model.Job;
import sidecar.ConsoleCapture;

@UserDefinedType(value = "etl_load")
public class Load extends ETLBase {

    @Column(value = "format")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String format;

    @Column(value = "coalesce")
    @CassandraType(type = CassandraType.Name.INT)
    public int coalesce = 1;

    @Column(value = "options")
    @CassandraType(type = CassandraType.Name.MAP, typeArguments = { CassandraType.Name.TEXT, CassandraType.Name.TEXT })
    public HashMap<String, String> options = new HashMap<>();

    @Column(value = "mode")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String mode;

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public int getCoalesce() {
        return coalesce;
    }

    public void setCoalesce(int coalesce) {
        this.coalesce = coalesce;
    }

    public HashMap<String, String> getOptions() {
        return options;
    }

    public void setOptions(HashMap<String, String> options) {
        this.options = options;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    @Override
    public void execute(Job job, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        String source = this.source != null ? this.source : "data";
        log().info("Loading " + source);

        if (format.contains("sql.jdbc")) {
            options.put("useSSL", "true");
            options.put("requireSSL", "true");
        }

        Dataset<Row> ds = dataset.get(source);
        if (coalesce > 0) {
            ds = ds.coalesce(coalesce);
        }

        if ("console".equals(format)) {
            Dataset<Row> out = ds;
            log().info("\n" + ConsoleCapture.capture(out::show));
        } else {
            if (format.contains("kafka")) {
                ds = ds.selectExpr("to_json(struct(*)) AS value");
            }

            Mode.of(mode).apply(job, spark, ds, this);
        }
    }

}
