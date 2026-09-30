package etl.model.extract;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.ETLBase;
import etl.model.Job;

@UserDefinedType(value = "etl_extract")
public class Extract extends ETLBase {

    @Column(value = "all")
    @CassandraType(type = CassandraType.Name.BOOLEAN)
    public Boolean all;

    @Column(value = "format")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String format;

    @Column(value = "options")
    @CassandraType(type = CassandraType.Name.MAP, typeArguments = { CassandraType.Name.TEXT, CassandraType.Name.TEXT })
    public HashMap<String, String> options = new HashMap<>();

    public Boolean getAll() {
        return all;
    }

    public void setAll(Boolean all) {
        this.all = all;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public HashMap<String, String> getOptions() {
        return options;
    }

    public void setOptions(HashMap<String, String> options) {
        this.options = options;
    }

    @Override
    public void execute(Job job, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        String target = this.target != null ? this.target : "data";
        log().info("Extracting " + format + " to " + target);

        if (format.contains("sql.jdbc")) {
            options.put("useSSL", "true");
            options.put("requireSSL", "true");
        }
        log().info("Options " + options);

        Dataset<Row> ds;

        if (all != null && all) {
            File dir = new File(options.get("path"));
            File[] files = dir.listFiles();
            Arrays.sort(files);

            ds = Arrays.stream(files)
                    .map(file -> spark.read().format(format).options(options).option("path", file.toString()).load())
                    .reduce(Dataset::union)
                    .orElse(null);
        } else if (format.contains("kafka") && "true".equals(options.get("stream"))) {
            ds = spark.readStream().format(format).options(options).load();
        } else {
            ds = spark.read().format(format).options(options).load();
        }

        if (format.contains("kafka")) {
            if ("true".equals(options.get("stream"))) {
                ds = ds.select(ds.col("value").cast(DataTypes.StringType));
            } else {
                ds = spark.read().json(ds.select(ds.col("value").cast(DataTypes.StringType)).as(Encoders.STRING()));
            }
        }

        ds = ds.na().drop("all");

        dataset.put(target, ds);
    }

}
