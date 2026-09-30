package etl.model.load;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;

import etl.model.Job;

/** Cassandra only load mode, issuing a DELETE per row keyed on the table's primary key. */
class DeleteMode implements Mode {

    @Override
    public void apply(Job job, SparkSession spark, Dataset<Row> ds, Load load) {

        String host = option(load, job, "spark.cassandra.connection.host");
        String user = option(load, job, "spark.cassandra.auth.username");
        String password = option(load, job, "spark.cassandra.auth.password");
        String dataCenter = option(load, job, "spark.cassandra.connection.local_dc");

        String keyspace = load.options.get("keyspace");
        String table = load.options.get("table");

        List<InetSocketAddress> inetSocketAddresses = new ArrayList<>();
        for (String contactPoint : host.split(",")) {
            inetSocketAddresses.add(new InetSocketAddress(contactPoint.trim(), 9042));
        }

        ds.foreachPartition((ForeachPartitionFunction<Row>) partition -> {

            DriverConfigLoader cassandraConfig = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(300))
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(300))
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(300))
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(300))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(300)).build();

            try (CqlSession cqlSession = CqlSession.builder()
                    .addContactPoints(inetSocketAddresses)
                    .withLocalDatacenter(dataCenter)
                    .withAuthCredentials(user, password)
                    .withKeyspace(keyspace)
                    .withConfigLoader(cassandraConfig)
                    .build()) {

                KeyspaceMetadata keyspaceMetadata = cqlSession.getMetadata().getKeyspace(keyspace).orElse(null);
                TableMetadata tableMetadata = keyspaceMetadata.getTable(table).orElse(null);
                List<ColumnMetadata> primaryKeys = tableMetadata.getPrimaryKey();

                deletePartition(cqlSession, partition, keyspace, table, primaryKeys);
            }
        });
    }

    private static void deletePartition(CqlSession cqlSession, Iterator<Row> partition, String keyspace, String table,
            List<ColumnMetadata> primaryKeys) {

        while (partition.hasNext()) {
            Row row = partition.next();

            StringBuilder query = new StringBuilder("DELETE FROM " + keyspace + "." + table + " WHERE ");
            boolean first = true;

            for (ColumnMetadata pkey : primaryKeys) {
                if (first) {
                    first = false;
                } else {
                    query.append(" AND ");
                }

                query.append(pkey.getName()).append("=");

                Object value = row.get(row.fieldIndex(pkey.getName().asInternal()));

                if (pkey.getType() == DataTypes.UUID) {
                    value = UUID.fromString(value.toString());
                }

                if (value instanceof String) {
                    query.append("'").append(value).append("'");
                } else {
                    query.append(value);
                }
            }

            query.append(";");
            cqlSession.execute(query.toString());
        }
    }

    private static String option(Load load, Job job, String key) {
        String value = load.options.get(key);
        return value != null ? value : (String) job.conf.get(key);
    }

}
