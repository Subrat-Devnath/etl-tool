package etl.model.load;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import etl.model.Job;

public interface Mode {

    void apply(Job job, SparkSession spark, Dataset<Row> ds, Load load);

    static Mode of(String s) {
        if (s == null) {
            return (job, spark, ds, load) -> {
                if (load.format.contains("kafka") && "true".equals(load.options.get("stream"))) {
                    try {
                        ds.writeStream().format(load.format).options(load.options).start();
                    } catch (TimeoutException e) {
                        throw new IllegalStateException(e);
                    }
                } else {
                    ds.write().format(load.format).options(load.options).save();
                }
            };
        }

        switch (s) {
            case "Append":
            case "Overwrite":
            case "ErrorIfExists":
            case "Ignore":
                return (job, spark, ds, load) -> ds.write().format(load.format).options(load.options).mode(s).save();

            case "Delete":
                return new DeleteMode();

            default:
                throw new IllegalArgumentException("Unsupported load mode - " + s);
        }
    }

}
