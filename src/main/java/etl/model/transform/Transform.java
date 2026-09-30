package etl.model.transform;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import etl.model.ETLBase;
import etl.model.Job;
import etl.model.transform.task.Add;
import etl.model.transform.task.Cast;
import etl.model.transform.task.Checksum;
import etl.model.transform.task.Count;
import etl.model.transform.task.Debug;
import etl.model.transform.task.Distinct;
import etl.model.transform.task.Drop;
import etl.model.transform.task.Explode;
import etl.model.transform.task.ExplodeToCol;
import etl.model.transform.task.Filter;
import etl.model.transform.task.FilterExpression;
import etl.model.transform.task.FromJson;
import etl.model.transform.task.Group;
import etl.model.transform.task.Join;
import etl.model.transform.task.Json;
import etl.model.transform.task.Rename;
import etl.model.transform.task.Select;
import etl.model.transform.task.Sort;
import etl.model.transform.task.Struct;
import etl.model.transform.task.Union;
import etl.model.transform.task.Xml;

@UserDefinedType(value = "etl_transform")
public class Transform extends ETLBase {

    @Column(value = "action_add")
    public Add add;
    @Column(value = "action_cast")
    public Cast cast;
    @Column(value = "action_drop")
    public Drop drop;
    @Column(value = "action_filter")
    public Filter filter;
    @Column(value = "action_filter_expression")
    public FilterExpression filterExpression;
    @Column(value = "action_group")
    public Group group;
    @Column(value = "action_join")
    public Join join;
    @Column(value = "action_rename")
    public Rename rename;
    @Column(value = "action_select")
    public Select select;
    @Column(value = "action_struct")
    public Struct struct;
    @Column(value = "action_explode")
    public Explode explode;
    @Column(value = "action_explode_to_col")
    public ExplodeToCol explodeToCol;
    @Column(value = "action_debug")
    public Debug debug;
    @Column(value = "action_json")
    public Json json;
    @Column(value = "action_distinct")
    public Distinct distinct;
    @Column(value = "action_union")
    public Union union;
    @Column(value = "action_count")
    public Count count;
    @Column(value = "action_xml")
    public Xml xml;
    @Column(value = "action_fromjson")
    public FromJson fromjson;
    @Column(value = "action_sort")
    public Sort sort;
    @Column(value = "action_checksum")
    public Checksum checksum;

    public TransformTask task() {
        if (select != null) {
            return select;
        } else if (cast != null) {
            return cast;
        } else if (filter != null) {
            return filter;
        } else if (filterExpression != null) {
            return filterExpression;
        } else if (group != null) {
            return group;
        } else if (join != null) {
            return join;
        } else if (add != null) {
            return add;
        } else if (rename != null) {
            return rename;
        } else if (drop != null) {
            return drop;
        } else if (struct != null) {
            return struct;
        } else if (explode != null) {
            return explode;
        } else if (explodeToCol != null) {
            return explodeToCol;
        } else if (debug != null) {
            return debug;
        } else if (json != null) {
            return json;
        } else if (distinct != null) {
            return distinct;
        } else if (union != null) {
            return union;
        } else if (count != null) {
            return count;
        } else if (xml != null) {
            return xml;
        } else if (fromjson != null) {
            return fromjson;
        } else if (sort != null) {
            return sort;
        } else if (checksum != null) {
            return checksum;
        }
        return null;
    }

    @Override
    public void execute(Job job, SparkSession spark, Map<String, Dataset<Row>> dataset) {
        String source = this.source != null ? this.source : "data";
        String target = this.target != null ? this.target : (this.source != null ? this.source : "data");

        TransformTask task = task();
        String operation = task.name();

        log().info("Transforming " + source + " to " + target + " with " + operation);

        Dataset<Row> in = dataset.get(source);
        Dataset<Row> out = task.execute(in, spark, dataset);

        dataset.put(target, out);
    }

    public Add getAdd() {
        return add;
    }

    public void setAdd(Add add) {
        this.add = add;
    }

    public Cast getCast() {
        return cast;
    }

    public void setCast(Cast cast) {
        this.cast = cast;
    }

    public Drop getDrop() {
        return drop;
    }

    public void setDrop(Drop drop) {
        this.drop = drop;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public FilterExpression getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(FilterExpression filterExpression) {
        this.filterExpression = filterExpression;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public Join getJoin() {
        return join;
    }

    public void setJoin(Join join) {
        this.join = join;
    }

    public Rename getRename() {
        return rename;
    }

    public void setRename(Rename rename) {
        this.rename = rename;
    }

    public Select getSelect() {
        return select;
    }

    public void setSelect(Select select) {
        this.select = select;
    }

    public Struct getStruct() {
        return struct;
    }

    public void setStruct(Struct struct) {
        this.struct = struct;
    }

    public Explode getExplode() {
        return explode;
    }

    public void setExplode(Explode explode) {
        this.explode = explode;
    }

    public ExplodeToCol getExplodeToCol() {
        return explodeToCol;
    }

    public void setExplodeToCol(ExplodeToCol explodeToCol) {
        this.explodeToCol = explodeToCol;
    }

    public Debug getDebug() {
        return debug;
    }

    public void setDebug(Debug debug) {
        this.debug = debug;
    }

    public Json getJson() {
        return json;
    }

    public void setJson(Json json) {
        this.json = json;
    }

    public Distinct getDistinct() {
        return distinct;
    }

    public void setDistinct(Distinct distinct) {
        this.distinct = distinct;
    }

    public Union getUnion() {
        return union;
    }

    public void setUnion(Union union) {
        this.union = union;
    }

    public Count getCount() {
        return count;
    }

    public void setCount(Count count) {
        this.count = count;
    }

    public Xml getXml() {
        return xml;
    }

    public void setXml(Xml xml) {
        this.xml = xml;
    }

    public FromJson getFromjson() {
        return fromjson;
    }

    public void setFromjson(FromJson fromjson) {
        this.fromjson = fromjson;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public Checksum getChecksum() {
        return checksum;
    }

    public void setChecksum(Checksum checksum) {
        this.checksum = checksum;
    }

}
