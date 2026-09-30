package etl.provider.impl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import etl.model.Job;
import etl.model.JobGroup;
import etl.provider.Provider;

public class YamlProvider extends Provider {

    @Override
    public String mode() {
        return "yaml";
    }

    @Override
    public JobGroup load() {
        Yaml yaml = new Yaml(new Constructor(JobGroup.class));
        String jobName = System.getProperty("job") == null ? "job" : System.getProperty("job");

        String job = jobName + ".yml";
        log().info("Loading Job - " + job);

        try {
            return (JobGroup) yaml.load(new FileInputStream(job));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Job file not found - " + job, e);
        }
    }

    @Override
    public void metadata(SparkSession spark, Job job) {
        Map<String, Object> metadata = job.metadata;

        if (metadata == null || metadata.isEmpty()) {
            return;
        }

        String cqlKeyspace = (String) metadata.get("cql.keyspace");
        if (cqlKeyspace == null) {
            return;
        }

        String environment = (String) metadata.get("env.profile");
        if (environment == null) {
            return;
        }

        CqlSession cqlSession = initCqlSession(job, cqlKeyspace);

        if (cqlSession != null) {
            try {
                addS3BucketCred(cqlKeyspace, environment, cqlSession, spark);
            } finally {
                cqlSession.close();
            }
        }
    }

    private CqlSession initCqlSession(Job job, String cqlKeyspace) {

        String cqlHost = null;
        Integer cqlPort = 9042;

        String cqlUser = null;
        String cqlPass = null;
        String dataCenter = null;

        Map<String, Object> config = job.conf;

        if (config != null && !config.isEmpty()) {
            if (config.get("spark.cassandra.connection.host") != null) {
                cqlHost = (String) config.get("spark.cassandra.connection.host");
            }
            if (config.get("spark.cassandra.connection.port") != null) {
                cqlPort = (Integer) config.get("spark.cassandra.connection.port");
            }
            if (config.get("spark.cassandra.auth.username") != null) {
                cqlUser = (String) config.get("spark.cassandra.auth.username");
            }
            if (config.get("spark.cassandra.auth.password") != null) {
                cqlPass = (String) config.get("spark.cassandra.auth.password");
            }
            if (config.get("spark.cassandra.connection.local_dc") != null) {
                dataCenter = (String) config.get("spark.cassandra.connection.local_dc");
            }
        }

        Map<String, Object> metadata = job.metadata;

        if (metadata != null && !metadata.isEmpty()) {
            if (cqlHost != null && metadata.get("spark.cassandra.connection.host") != null) {
                cqlHost = (String) metadata.get("spark.cassandra.connection.host");
            }
            if (cqlPort != null && metadata.get("spark.cassandra.connection.port") != null) {
                cqlPort = (Integer) metadata.get("spark.cassandra.connection.port");
            }
            if (cqlUser != null && metadata.get("spark.cassandra.auth.username") != null) {
                cqlUser = (String) metadata.get("spark.cassandra.auth.username");
            }
            if (cqlPass != null && metadata.get("spark.cassandra.auth.password") != null) {
                cqlPass = (String) metadata.get("spark.cassandra.auth.password");
            }
        }

        log().info("Connecting to cassandra cluster with contact points- " + cqlHost);

        if (!cqlHost.isEmpty() && !cqlUser.isEmpty() && !cqlPass.isEmpty() && !cqlKeyspace.isEmpty()) {

            List<InetSocketAddress> inetSocketAddresses = new ArrayList<>();
            for (String contactPoint : cqlHost.split(",")) {
                inetSocketAddresses.add(new InetSocketAddress(contactPoint.trim(), cqlPort));
            }

            CqlSession session = CqlSession.builder()
                    .addContactPoints(inetSocketAddresses)
                    .withLocalDatacenter(dataCenter)
                    .withAuthCredentials(cqlUser, cqlPass)
                    .withKeyspace(cqlKeyspace)
                    .withConfigLoader(cassandraConfig())
                    .build();

            log().info("Connected to cassandra cluster with contact points - " + cqlHost);
            return session;
        }
        return null;
    }

    private DriverConfigLoader cassandraConfig() {
        return DriverConfigLoader.programmaticBuilder()
                /*
                 * Resolves the timeout query 'SELECT * FROM system_schema.tables' timed out
                 * after PT2S
                 */
                .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(300))

                /*
                 * Timeout for schema-related queries during the initialization phase (e.g.,
                 * fetching system tables). This ensures that the driver waits up to 5 minutes
                 * for such queries to complete, which can be helpful if the CASSANDRA cluster
                 * is under load or the network is slow.
                 */
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(300))

                /*
                 * Connection timeout to establish a connection with a CASSANDRA node. The
                 * driver will attempt to connect for up to 5 minutes before throwing a timeout
                 * exception.
                 */
                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(300))

                /*
                 * Resolves the timeout query 'SELECT * FROM system.peers' timed out after
                 * PT0.5S
                 */
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(300))

                /*
                 * Request timeout for regular CQL queries. This defines the maximum amount of
                 * time the driver will wait for a query to complete before throwing a timeout
                 * exception. The timeout is set to 5 minutes to accommodate long-running
                 * queries or network delays.
                 */
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(300)).build();
    }

}
