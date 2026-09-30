package etl.model.transform.task;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.transform.TransformTask;

@UserDefinedType(value = "etl_join")
public class Join extends TransformTask {

    private static final long serialVersionUID = 1L;

    @Column(value = "right")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String right;

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> col;

    @Column(value = "joinType")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String joinType = "inner";

    @Override
    public String name() {
        return "join";
    }

    @Override
    public Dataset<Row> execute(Dataset<Row> ds, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        return ds.join(dataset.get(right), col.toArray(new String[0]), joinType);
    }

    public String getRight() {
        return right;
    }

    public void setRight(String right) {
        this.right = right;
    }

    public List<String> getCol() {
        return col;
    }

    public void setCol(List<String> col) {
        this.col = col;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

}
