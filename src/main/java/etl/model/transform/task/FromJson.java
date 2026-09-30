package etl.model.transform.task;

import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_from_json")
public class FromJson extends TransformTask {

    private static final long serialVersionUID = 1L;

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @Column(value = "rootArray")
    @CassandraType(type = CassandraType.Name.BOOLEAN)
    public boolean rootArray = false;

    @Override
    public String name() {
        return "from_json";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        Dataset<String> json = ds.select(col).map((MapFunction<Row, String>) row -> row.mkString(""), Encoders.STRING());
        Dataset<Row> jsonDs = spark.read().json(json);

        DataType schema = rootArray ? DataTypes.createArrayType(jsonDs.schema()) : jsonDs.schema();

        return ds.withColumn(col, functions.from_json(ds.col(col), schema));
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public boolean isRootArray() {
        return rootArray;
    }

    public void setRootArray(boolean rootArray) {
        this.rootArray = rootArray;
    }

}
