package base;

import org.apache.spark.sql.SparkSession;

public abstract class ETL {

    protected final SparkSession spark;

    protected ETL(SparkSession spark) {
        this.spark = spark;
    }

    public abstract void run();

}
