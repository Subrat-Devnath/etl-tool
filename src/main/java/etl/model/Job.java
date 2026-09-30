package etl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Table;

import etl.model.extract.Extract;
import etl.model.load.Load;
import etl.model.transform.Transform;

@Table(value = "etl_job")
public class Job {

    @Column(value = "name")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String name;

    /**
     * Holds whatever yaml/json parsed into: spark settings are strings or ints, and
     * status.callback is a nested map.
     */
    @Column(value = "conf")
    @CassandraType(type = CassandraType.Name.MAP, typeArguments = { CassandraType.Name.TEXT, CassandraType.Name.TEXT })
    public Map<String, Object> conf = new HashMap<>();

    @Column(value = "etl_conf")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String etl_conf;

    @Column(value = "extract")
    public List<Extract> extract;

    @Column(value = "transform")
    public List<Transform> transform;

    @Column(value = "load")
    public List<Load> load;

    @Column(value = "status")
    @CassandraType(type = CassandraType.Name.TEXT)
    public String status;

    @Column(value = "metadata")
    @CassandraType(type = CassandraType.Name.MAP, typeArguments = { CassandraType.Name.TEXT, CassandraType.Name.TEXT })
    public Map<String, Object> metadata = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getConf() {
        return conf;
    }

    public void setConf(Map<String, Object> conf) {
        this.conf = conf;
    }

    public String getEtl_conf() {
        return etl_conf;
    }

    public void setEtl_conf(String etl_conf) {
        this.etl_conf = etl_conf;
    }

    public List<Extract> getExtract() {
        return extract;
    }

    public void setExtract(List<Extract> extract) {
        this.extract = extract;
    }

    public List<Transform> getTransform() {
        return transform;
    }

    public void setTransform(List<Transform> transform) {
        this.transform = transform;
    }

    public List<Load> getLoad() {
        return load;
    }

    public void setLoad(List<Load> load) {
        this.load = load;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

}
