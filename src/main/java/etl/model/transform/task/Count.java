package etl.model.transform.task;

import java.util.Collections;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_count")
public class Count extends TransformTask {

    private static final long serialVersionUID = 1L;

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @Override
    public String name() {
        return "count";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        return spark.createDataset(Collections.singletonList(ds.count()), Encoders.LONG())
                .toDF()
                .withColumnRenamed("value", col);
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

}
