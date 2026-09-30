package etl.model.transform.task;

import java.util.List;

import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

@UserDefinedType(value = "etl_agg")
public class Agg {

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> col;

    @Column(value = "agg")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String agg;

    @Column(value = "alias")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String alias;

    public List<String> getCol() {
        return col;
    }

    public void setCol(List<String> col) {
        this.col = col;
    }

    public String getAgg() {
        return agg;
    }

    public void setAgg(String agg) {
        this.agg = agg;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

}
