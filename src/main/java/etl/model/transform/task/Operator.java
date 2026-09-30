package etl.model.transform.task;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public interface Operator {

    Column apply(Column col, Dataset<Row> ds, Filter filter);

    static Column toCol(Dataset<Row> ds, Filter filter) {
        return filter.value != null ? functions.lit(filter.value) : ds.col(filter.ref);
    }

    static Operator of(String s) {
        switch (s) {
            case "NULL":
                return (col, ds, filter) -> col.isNull();

            case "EQ":
                return (col, ds, filter) -> col.equalTo(toCol(ds, filter));

            case "GT":
                return (col, ds, filter) -> col.gt(toCol(ds, filter));

            case "LT":
                return (col, ds, filter) -> col.lt(toCol(ds, filter));

            case "GEQ":
                return (col, ds, filter) -> col.geq(toCol(ds, filter));

            case "LEQ":
                return (col, ds, filter) -> col.leq(toCol(ds, filter));

            case "NAN":
                return (col, ds, filter) -> col.isNaN();

            case "LIKE":
                return (col, ds, filter) -> col.like(filter.value);

            case "RLIKE":
                return (col, ds, filter) -> col.rlike(filter.value);

            case "STARTS":
                return (col, ds, filter) -> col.startsWith(toCol(ds, filter));

            case "ENDS":
                return (col, ds, filter) -> col.endsWith(toCol(ds, filter));

            default:
                throw new IllegalArgumentException("Unsupported filter operator - " + s);
        }
    }

}
