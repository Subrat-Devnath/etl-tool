package etl.model.transform.task;

import java.util.List;

import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

@UserDefinedType(value = "etl_udf")
public class Udf {

    @Column(value = "col")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> col;

    @Column(value = "function")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String function;

    @Column(value = "params")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = { CassandraType.Name.TEXT })
    public List<String> params;

    public List<String> getCol() {
        return col;
    }

    public void setCol(List<String> col) {
        this.col = col;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public List<String> getParams() {
        return params;
    }

    public void setParams(List<String> params) {
        this.params = params;
    }

}
