package etl.model.transform.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_add")
public class Add extends TransformTask {

    private static final long serialVersionUID = 1L;

    @org.springframework.data.cassandra.core.mapping.Column(value = "col")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String col;

    @org.springframework.data.cassandra.core.mapping.Column(value = "udf")
    public Udf udf;

    @org.springframework.data.cassandra.core.mapping.Column(value = "value")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String value;

    @org.springframework.data.cassandra.core.mapping.Column(value = "ref")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String ref;

    @org.springframework.data.cassandra.core.mapping.Column(value = "condition")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String condition;

    @Override
    public String name() {
        return "add";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        Column column = null;

        if (udf != null) {

            List<Column> args = new ArrayList<>();
            if (udf.params != null) {
                udf.params.forEach(param -> args.add(functions.lit(param)));
            }
            if (udf.col != null) {
                udf.col.forEach(name -> args.add(ds.col(name)));
            }
            if (this.value != null) {
                args.add(functions.lit(this.value));
            }

            column = functions.call_udf(udf.function, args.toArray(new Column[0]));

        } else if (condition != null) {

            if (ref != null) {
                column = ds.col(ref);
            } else if (this.value != null) {
                column = functions.lit(this.value);
            }

            if (column != null) {
                column = functions.when(functions.expr(condition), column);
            } else {
                column = functions.expr(condition);
            }

        } else if (ref != null) {
            try {
                column = ds.col(ref);
            } catch (Exception e) {
                column = functions.lit(null);
            }
        } else {
            column = functions.lit(this.value);
        }

        if (col.contains(".")) {
            return NestedColumns.addNestedColumn(ds, col, column);
        }
        return ds.withColumn(col, column);
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public Udf getUdf() {
        return udf;
    }

    public void setUdf(Udf udf) {
        this.udf = udf;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

}
