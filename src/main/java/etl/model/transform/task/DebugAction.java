package etl.model.transform.task;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import sidecar.ConsoleCapture;

public interface DebugAction {

    org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(DebugAction.class.getName());

    void apply(Dataset<Row> ds);

    static DebugAction of(String s) {
        switch (s) {
            case "count":
                return ds -> log.info("\nNumber of rows - " + ds.count());

            case "schema":
                return ds -> log.info("\n" + ConsoleCapture.capture(ds::printSchema));

            case "data":
                return ds -> log.info("\n" + ConsoleCapture.capture(() -> ds.show(false)));

            default:
                throw new IllegalArgumentException("Unsupported debug action - " + s);
        }
    }

}
