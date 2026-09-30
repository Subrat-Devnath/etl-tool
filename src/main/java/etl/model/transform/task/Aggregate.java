package etl.model.transform.task;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public interface Aggregate {

    Column apply(Column col);

    static Aggregate of(String s) {
        switch (s) {
            case "avg":
                return functions::avg;

            case "collect_list":
                return functions::collect_list;

            case "collect_set":
                return functions::collect_set;

            case "count":
                return functions::count;

            case "countDistinct":
                return functions::countDistinct;

            case "first":
                return functions::first;

            case "last":
                return functions::last;

            case "max":
                return functions::max;

            case "min":
                return functions::min;

            case "sum":
                return functions::sum;

            case "sumDistinct":
                return functions::sum_distinct;

            case "firstNotNull":
                return col -> functions.first(col, true);

            default:
                throw new IllegalArgumentException("Unsupported aggregate function - " + s);
        }
    }

}
