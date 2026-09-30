package etl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

public final class UDF {

    private UDF() {
    }

    public static void registerUDF(SparkSession spark) {
        spark.udf().register("int", (UDF1<Object, Integer>) UDF::parseInt, DataTypes.IntegerType);
        spark.udf().register("long", (UDF1<Object, Long>) UDF::parseLong, DataTypes.LongType);
        spark.udf().register("boolean", (UDF1<Object, Boolean>) UDF::parseBoolean, DataTypes.BooleanType);
        spark.udf().register("double", (UDF1<Object, Double>) UDF::parseDouble, DataTypes.DoubleType);
        spark.udf().register("binary", (UDF1<Object, byte[]>) UDF::parseBinary, DataTypes.BinaryType);
        spark.udf().register("date", (UDF2<Object, Object, Timestamp>) UDF::parseDate, DataTypes.TimestampType);
        spark.udf().register("dateToString", (UDF2<Object, Object, String>) UDF::formatDate, DataTypes.StringType);
        spark.udf().register("timestamp", (UDF1<Object, Long>) UDF::timestamp, DataTypes.LongType);
        spark.udf().register("now", (UDF0<Timestamp>) UDF::now, DataTypes.TimestampType);
        spark.udf().register("nowTimestamp", (UDF0<Long>) UDF::nowTimestamp, DataTypes.LongType);
        spark.udf().register("coterm",
                (UDF4<Object, Object, Object, Object, Double>) UDF::cotermedValue, DataTypes.DoubleType);
        spark.udf().register("uuid", (UDF0<String>) UDF::uuid, DataTypes.StringType);
        spark.udf().register("structToString", (UDF2<Object, Object, String>) UDF::structToString,
                DataTypes.StringType);
        spark.udf().register("seqToString", (UDF2<Object, Object, String>) UDF::seqToString, DataTypes.StringType);
        spark.udf().register("col_to_json", (UDF1<Object, String>) UDF::colToJson, DataTypes.StringType);
        spark.udf().register("stringToSeq", (UDF2<Object, Object, String[]>) UDF::stringToSeq,
                DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("replace", (UDF3<Object, Object, Object, String>) UDF::replace, DataTypes.StringType);
        spark.udf().register("split", (UDF3<Object, Object, Object, String>) UDF::split, DataTypes.StringType);
        spark.udf().register("blankAsNull", (UDF1<Object, String>) UDF::blankAsNull, DataTypes.StringType);
        spark.udf().register("id", (UDF0<Long>) UDF::id, DataTypes.LongType);
    }

    public static String colToJson(Object obj) {
        StringBuilder out = new StringBuilder();

        if (isNumeric(obj.toString())) {
            out.append(obj);
        } else if (obj instanceof scala.collection.Seq || obj instanceof List) {
            StringBuilder arrayOut = new StringBuilder();
            for (Object elem : asJavaList(obj)) {
                if (arrayOut.length() > 0) {
                    arrayOut.append(",");
                }
                arrayOut.append(colToJson(elem));
            }
            out.append("[").append(arrayOut).append("]");
        } else if (obj instanceof Row) {
            Row row = (Row) obj;
            StringBuilder rowOut = new StringBuilder();
            for (org.apache.spark.sql.types.StructField cell : row.schema().fields()) {
                if (rowOut.length() > 0) {
                    rowOut.append(",");
                }
                rowOut.append("\"").append(cell.name()).append("\":");
                rowOut.append(colToJson(row.get(row.fieldIndex(cell.name()))));
            }
            out.append("{").append(rowOut).append("}");
        } else {
            out.append("\"").append(obj).append("\"");
        }

        return out.toString();
    }

    public static boolean isNumeric(String str) {
        return str.matches("[-+]?\\d+(\\.\\d+)?");
    }

    public static String[] stringToSeq(Object separator, Object data) {
        String value = asString(data);
        return value == null ? null : value.split(asString(separator));
    }

    public static String replace(Object from, Object to, Object data) {
        String value = asString(data);
        return value == null ? null : value.replaceAll(asString(from), asString(to));
    }

    public static Boolean parseBoolean(Object obj) {
        boolean out = false;
        String str = asString(obj);
        if (str != null) {
            out = toBoolean(str);
        }
        return out;
    }

    public static Integer parseInt(Object obj) {
        String str = asString(obj);
        return str == null ? null : Integer.valueOf(str.replaceAll("[^0-9.]", ""));
    }

    public static Long parseLong(Object obj) {
        String str = asString(obj);
        return str == null ? null : Long.valueOf(str.replaceAll("[^0-9.]", ""));
    }

    public static Double parseDouble(Object obj) {
        String str = asString(obj);
        return str == null ? null : Double.valueOf(str.replaceAll("[^0-9.]", ""));
    }

    public static byte[] parseBinary(Object obj) {
        return obj.toString().getBytes();
    }

    public static Timestamp parseDate(Object formatObj, Object obj) {
        String str = asString(obj);
        if (str == null) {
            return null;
        }

        String format = asString(formatObj);

        if ("timestamp".equals(format)) {
            return new Timestamp(Long.parseLong(str));
        }

        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            sdf.setLenient(false);
            return new Timestamp(sdf.parse(str).getTime());
        } catch (Exception e) {
            return null;
        }
    }

    public static String formatDate(Object formatObj, Object dateObj) {
        String format = asString(formatObj);
        Timestamp date = asTimestamp(dateObj);

        if (format == null || date == null) {
            return null;
        }
        return new SimpleDateFormat(format).format(date);
    }

    public static String structToString(Object separatorObj, Object rowObj) {
        Row row = (Row) rowObj;

        List<String> values = new ArrayList<>();
        for (int i = 0; i < row.length(); i++) {
            Object value = row.get(i);
            if (value != null) {
                values.add(String.valueOf(value));
            }
        }
        return String.join(asString(separatorObj), values);
    }

    public static String seqToString(Object separatorObj, Object seqObj) {
        String separator = asString(separatorObj);

        if (separator != null && seqObj != null) {
            List<Object> seq = asJavaList(seqObj);

            if (seq != null) {
                List<String> values = new ArrayList<>();
                for (Object value : seq) {
                    if (value != null) {
                        values.add(String.valueOf(value));
                    }
                }
                return String.join(separator, values);
            }
        }
        return null;
    }

    public static Double cotermedValue(Object startObj, Object endObj, Object cotermedDateObj, Object amountObj) {
        Timestamp start = asTimestamp(startObj);
        Timestamp end = asTimestamp(endObj);
        Timestamp cotermedDate = asTimestamp(cotermedDateObj);
        double amount = asDouble(amountObj);

        return amount * (cotermedDate.getTime() - start.getTime()) / (end.getTime() - start.getTime());
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static Long id() {
        return (long) (Math.random() * Long.MAX_VALUE);
    }

    public static Timestamp now() {
        return new Timestamp(Calendar.getInstance().getTime().getTime());
    }

    public static Long nowTimestamp() {
        return Calendar.getInstance().getTime().getTime();
    }

    public static Long timestamp(Object dateObj) {
        return asTimestamp(dateObj).getTime();
    }

    public static String split(Object splitObj, Object indexObj, Object obj) {
        String s = asString(obj);
        if (s == null || s.isEmpty()) {
            return null;
        }

        int index = asInt(indexObj);
        String[] splitted = s.split(asString(splitObj));

        return splitted.length >= index ? splitted[index] : null;
    }

    public static String blankAsNull(Object obj) {
        String s = asString(obj);
        return "".equals(s) ? null : s;
    }

    private static boolean toBoolean(String s) {
        switch (s.toLowerCase()) {
            case "true":
                return true;
            case "false":
                return false;
            default:
                throw new IllegalArgumentException("For input string: \"" + s + "\"");
        }
    }

    /**
     * Java UDFs are registered without input encoders, so Spark does not coerce the
     * argument types the way it did for the Scala function UDFs this replaced.
     */
    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof String ? (String) value : value.toString();
    }

    private static int asInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(asString(value));
    }

    private static double asDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(asString(value));
    }

    private static Timestamp asTimestamp(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof java.time.Instant) {
            return Timestamp.from((java.time.Instant) value);
        }
        if (value instanceof Number) {
            return new Timestamp(((Number) value).longValue());
        }
        return Timestamp.valueOf(asString(value));
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asJavaList(Object value) {
        if (value instanceof scala.collection.Seq) {
            return new ArrayList<>(
                    scala.collection.JavaConverters.seqAsJavaList((scala.collection.Seq<Object>) value));
        }
        if (value instanceof List) {
            return (List<Object>) value;
        }
        if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        }
        return null;
    }

}
