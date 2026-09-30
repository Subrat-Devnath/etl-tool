package etl.model.transform.task;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_checksum")
public class Checksum extends TransformTask {

    private static final long serialVersionUID = 1L;

    /** particular column to include for checksum */
    @org.springframework.data.cassandra.core.mapping.Column(value = "col")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> col;

    /** checksum column name */
    @org.springframework.data.cassandra.core.mapping.Column(value = "target")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String target;

    /** exclude given column */
    @org.springframework.data.cassandra.core.mapping.Column(value = "exclude")
    @CassandraType(type = CassandraType.Name.BOOLEAN)
    public Boolean exclude;

    @Override
    public String name() {
        return "checksum";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        UserDefinedFunction checksumUDF = functions.udf(
                (UDF1<Row, String>) Checksum::calculateChecksum, DataTypes.StringType);

        List<String> columnNames;

        if (col != null && !col.isEmpty()) {
            if (exclude != null && exclude) {
                columnNames = new ArrayList<>();
                for (String name : ds.columns()) {
                    if (!col.contains(name)) {
                        columnNames.add(name);
                    }
                }
            } else {
                columnNames = col;
            }
        } else {
            columnNames = Arrays.asList(ds.columns());
        }

        Column[] selectColumns = columnNames.stream().map(functions::col).toArray(Column[]::new);

        return ds.withColumn(target, checksumUDF.apply(functions.struct(selectColumns)));
    }

    public static String calculateChecksum(Row row) {
        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }

        for (int i = 0; i < row.length(); i++) {
            Object field = row.get(i);
            if (field != null) {
                sha256.update(field.toString().getBytes());
            }
        }

        StringBuilder checksum = new StringBuilder();
        for (byte b : sha256.digest()) {
            checksum.append(String.format("%02x", b));
        }
        return checksum.toString();
    }

    public List<String> getCol() {
        return col;
    }

    public void setCol(List<String> col) {
        this.col = col;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Boolean getExclude() {
        return exclude;
    }

    public void setExclude(Boolean exclude) {
        this.exclude = exclude;
    }

}
