package etl.model.transform.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_explode_to_col")
public class ExplodeToCol extends TransformTask {

    private static final long serialVersionUID = 1L;

    @org.springframework.data.cassandra.core.mapping.Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @org.springframework.data.cassandra.core.mapping.Column(value = "keep")
    @CassandraType(type = CassandraType.Name.BOOLEAN)
    public Boolean keep;

    @Override
    public String name() {
        return "explode_to_col";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        // Threaded through as a local AtomicReference (rather than an instance field) so this
        // per-execution scratch state never leaks into the entity's own fields - a plain
        // Dataset<Row> instance field here previously made Spring Data's Cassandra row mapper
        // (CassandraConverter/CassandraMappingContext, used to remove hand-written row.getX(...)
        // boilerplate) recurse infinitely while inspecting Dataset's generic type signature.
        AtomicReference<Dataset<Row>> dataSetRef = new AtomicReference<>(ds);

        DataType dataType = schema(dataSetRef, col).dataType();

        if (dataType instanceof StructType) {
            if (keep == null || !keep) {
                dataSetRef.set(dataSetRef.get().drop(col));
            }
            return dataSetRef.get();
        }
        return ds;
    }

    public StructField schema(AtomicReference<Dataset<Row>> dataSetRef, String col) {
        for (StructField field : flatten(dataSetRef, dataSetRef.get().schema(), "")) {
            if (field.name().equals(col)) {
                return field;
            }
        }
        return null;
    }

    /**
     * Walks the original schema and, for every direct child of {@code col}, lifts that
     * child onto {@code dataSetRef} as a top level column.
     */
    public List<StructField> flatten(AtomicReference<Dataset<Row>> dataSetRef, StructType schema, String prefix) {
        List<StructField> flattened = new ArrayList<>();

        for (StructField f : schema.fields()) {
            String newName = prefix + f.name();

            if (prefix.equals(col + ".")) {
                Column value = dataSetRef.get().col(newName);
                dataSetRef.set(NestedColumns.addNestedColumn(dataSetRef.get(), f.name(), value));
            }

            flattened.add(new StructField(newName, f.dataType(), f.nullable(), f.metadata()));

            if (f.dataType() instanceof StructType) {
                flattened.addAll(flatten(dataSetRef, (StructType) f.dataType(), newName + "."));
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

    public Boolean getKeep() {
        return keep;
    }

    public void setKeep(Boolean keep) {
        this.keep = keep;
    }

}
