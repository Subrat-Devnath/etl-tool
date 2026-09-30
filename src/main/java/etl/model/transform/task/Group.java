package etl.model.transform.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_group")
public class Group extends TransformTask {

    private static final long serialVersionUID = 1L;

    @org.springframework.data.cassandra.core.mapping.Column(value = "col")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> col;

    @org.springframework.data.cassandra.core.mapping.Column(value = "agg")
    public List<Agg> agg;

    @org.springframework.data.cassandra.core.mapping.Column(value = "def_agg")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String def_agg;

    @Override
    public String name() {
        return "group";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        List<Column> aggs = new ArrayList<>();

        List<String> col = this.col != null ? this.col : Collections.emptyList();
        List<Agg> agg = this.agg != null ? this.agg : Collections.emptyList();

        if (def_agg != null) {
            for (String c : ds.columns()) {
                boolean skip = col.contains(c);

                if (!skip) {
                    for (Agg a : agg) {
                        if (a.col.contains(c)) {
                            skip = true;
                        }
                    }
                }

                if (!skip) {
                    aggs.add(Aggregate.of(def_agg).apply(ds.col(c)).as(c));
                }
            }
        }

        for (Agg a : agg) {
            Column column = Aggregate.of(a.agg).apply(ds.col(a.col.get(0)));
            aggs.add(a.alias != null ? column.as(a.alias) : column.as(a.col.get(0)));
        }

        String[] groupRest = col.subList(1, col.size()).toArray(new String[0]);
        Column[] aggRest = aggs.subList(1, aggs.size()).toArray(new Column[0]);

        return ds.groupBy(col.get(0), groupRest).agg(aggs.get(0), aggRest);
    }

    public List<String> getCol() {
        return col;
    }

    public void setCol(List<String> col) {
        this.col = col;
    }

    public List<Agg> getAgg() {
        return agg;
    }

    public void setAgg(List<Agg> agg) {
        this.agg = agg;
    }

    public String getDef_agg() {
        return def_agg;
    }

    public void setDef_agg(String def_agg) {
        this.def_agg = def_agg;
    }

}
