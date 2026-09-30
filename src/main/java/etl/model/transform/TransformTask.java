package etl.model.transform;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class TransformTask implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract String name();

    public abstract Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset);

}
