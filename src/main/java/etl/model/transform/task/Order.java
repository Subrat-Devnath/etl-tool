package etl.model.transform.task;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public interface Order {

    Column apply(String col);

    static Order of(String order) {
        switch (order) {
            case "asc":
                return functions::asc;

            case "asc_nulls_first":
                return functions::asc_nulls_first;

            case "asc_nulls_last":
                return functions::asc_nulls_last;

            case "desc":
                return functions::desc;

            case "desc_nulls_first":
                return functions::desc_nulls_first;

            case "desc_nulls_last":
                return functions::desc_nulls_last;

            default:
                throw new IllegalArgumentException("Unsupported sort order - " + order);
        }
    }

}
