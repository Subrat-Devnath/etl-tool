package etl.model.transform.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_explode")
public class Explode extends TransformTask {

    private static final long serialVersionUID = 1L;

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @Column(value = "nullable")
    @CassandraType(type = CassandraType.Name.BOOLEAN)
    public Boolean nullable = false;

    @Override
    public String name() {
        return "explode";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        DataType dataType = schema(ds.schema(), col).dataType();

        if (dataType instanceof ArrayType || dataType instanceof MapType) {
            if (nullable != null && nullable) {
                return ds.withColumn(col, functions.explode_outer(ds.col(col)));
            }
            return ds.withColumn(col, functions.explode(ds.col(col)));
        }
        return ds;
    }

    public StructField schema(StructType schema, String col) {
        for (StructField field : flatten(schema, "")) {
            if (field.name().equals(col)) {
                return field;
            }
        }
        return null;
    }

    public List<StructField> flatten(StructType schema, String prefix) {
        List<StructField> flattened = new ArrayList<>();

        for (StructField f : schema.fields()) {
            String newName = prefix + f.name();
            flattened.add(new StructField(newName, f.dataType(), f.nullable(), f.metadata()));

            if (f.dataType() instanceof StructType) {
                flattened.addAll(flatten((StructType) f.dataType(), newName + "."));
            }
        }

        return flattened;
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

}
