package etl.model.transform.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Adds a column addressed by a dotted path, rebuilding every struct along the way.
 * Shared by the add and explode_to_col tasks.
 */
final class NestedColumns {

    private NestedColumns() {
    }

    static Dataset<Row> addSubLevelColumn(Dataset<Row> ds, String col, Column value) {
        String prefix = col.substring(0, col.indexOf("."));
        String suffix = col.substring(col.indexOf(".") + 1);

        List<Column> structCols = new ArrayList<>();
        for (String name : ds.select(prefix + ".*").columns()) {
            if (!name.equals(suffix)) {
                structCols.add(ds.col(prefix + "." + name));
            }
        }
        structCols.add(value.as(suffix));

        return ds.withColumn(prefix, functions.struct(structCols.toArray(new Column[0])));
    }

    static Dataset<Row> addNestedColumn(Dataset<Row> df, String newColName, Column newCol) {
        if (!newColName.contains(".")) {
            // Top level addition, use spark method as-is
            return df.withColumn(newColName, newCol);
        }

        List<String> splitted = Arrays.asList(newColName.split("\\."));
        String head = splitted.get(0);
        List<String> tail = splitted.subList(1, splitted.size());

        for (StructField f : df.schema().fields()) {
            if (f.name().equals(head)) {
                Column modified = recursiveAddNestedColumn(tail, functions.col(f.name()), f.dataType(), f.nullable(),
                        newCol);
                return df.withColumn(f.name(), modified);
            }
        }

        return df.withColumn(head, createNestedStructs(tail, newCol).as(head));
    }

    private static Column recursiveAddNestedColumn(List<String> splitted, Column col, DataType colType,
            boolean nullable, Column newCol) {

        if (!(colType instanceof StructType) || splitted.isEmpty()) {
            return createNestedStructs(splitted, newCol);
        }

        String head = splitted.get(0);
        List<String> tail = splitted.subList(1, splitted.size());

        List<Column> modifiedFields = new ArrayList<>();
        boolean found = false;

        for (StructField f : ((StructType) colType).fields()) {
            Column curCol = col.getField(f.name());
            if (f.name().equals(head)) {
                found = true;
                curCol = recursiveAddNestedColumn(tail, curCol, f.dataType(), f.nullable(), newCol);
            }
            modifiedFields.add(curCol.as(f.name()));
        }

        if (!found) {
            modifiedFields.add(nullableCol(col, createNestedStructs(tail, newCol)).as(head));
        }

        Column modifiedStruct = functions.struct(modifiedFields.toArray(new Column[0]));
        if (nullable) {
            modifiedStruct = nullableCol(col, modifiedStruct);
        }
        return modifiedStruct;
    }

    private static Column createNestedStructs(List<String> splitted, Column newCol) {
        Column nestedStruct = newCol;
        for (int i = splitted.size() - 1; i >= 0; i--) {
            nestedStruct = nullableCol(functions.struct(nestedStruct.as(splitted.get(i))));
        }
        return nestedStruct;
    }

    private static Column nullableCol(Column parentCol, Column c) {
        return functions.when(parentCol.isNotNull(), c);
    }

    private static Column nullableCol(Column c) {
        return nullableCol(c, c);
    }

}
