package etl.provider.impl;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.sql.SparkSession;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import etl.model.Job;
import etl.model.JobGroup;
import etl.model.JobStatus;
import etl.provider.Provider;

public class CassandraProvider extends Provider {

	private final UUID jobId = UUID.fromString(System.getProperty("job"));
	private final String cqlKeyspace = System.getProperty("cql_keyspace");
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public String mode() {
		return "cassandra";
	}

	public CqlSession initCqlSession() {
		log().info("Init session " + jobId + " " + cqlKeyspace);

		// Load configuration
		Config config = ConfigFactory.load();

		String cqlHost = System.getProperty("cql_host");
		Integer cqlPort = System.getProperty("cql_port") == null
				? 9042
				: Integer.parseInt(System.getProperty("cql_port"));

		String cqlUser = config.hasPath("database.cassandra.user")
				? config.getString("database.cassandra.user")
				: System.getProperty("cql_user");
		String cqlPass = config.hasPath("database.cassandra.pass")
				? config.getString("database.cassandra.pass")
				: System.getProperty("cql_pass");

		String dataCenter = System.getProperty("cql_data_center");

		log().info("Connecting to cassandra cluster with contact points - " + cqlHost);

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

	@Override
	public void updateStatus(Job job, JobStatus status) {

		log().info("Updating job " + job.name + " status " + status);

		try (CqlSession cqlSession = initCqlSession()) {
			cqlSession.execute("update etl_job set status='" + status.toString().trim() + "' where id=" + jobId);
		} finally {
			log().info("Updated job with Query " + job.name + " status " + status);
		}

		super.updateStatus(job, status);
	}

	@Override
	@SuppressWarnings("unchecked")
	public JobGroup load() {

		log().info("Running job from cassandra - keyspace - " + cqlKeyspace + " job - " + jobId);

		try (CqlSession cqlSession = initCqlSession()) {

			ResultSet resultSet = cqlSession.execute("SELECT * FROM etl_job WHERE id = " + jobId);

			initCassandraConverter(cqlSession);

			// CassandraConverter#read() maps the row onto Job by field name (name, status,
			// extract, transform, load, ...), so there's no hand-written row.getX(...) per field.
			List<Job> jobs = (List<Job>) getEntitiesFromResultSet(resultSet, Job.class);
			Job etlJob = jobs.get(0);

			if (etlJob.etl_conf != null) {
				etlJob.conf = readConf(etlJob.etl_conf);
			}

			JobGroup jobGroup = new JobGroup();
			jobGroup.jobs = new Job[]{etlJob};
			jobGroup.skipErrors = true;
			return jobGroup;
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> readConf(String etlConf) {
		try {
			return objectMapper.readValue(etlConf, Map.class);
		} catch (Exception e) {
			throw new IllegalStateException("Unable to parse etl_conf", e);
		}
	}

	@Override
	public void metadata(SparkSession spark, Job job) {

		Map<String, Object> metadata = job.metadata;

		if (metadata.get("env.profile") == null) {
			return;
		}

		String environment = (String) metadata.get("env.profile");

		if (environment == null) {
			return;
		}

		CqlSession cqlSession = initCqlSession();
		addS3BucketCred(cqlKeyspace, environment, cqlSession, spark);
	}

	private DriverConfigLoader cassandraConfig() {
		return DriverConfigLoader.programmaticBuilder()
				.withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(300))
				.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(300))
				.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(300))
				.withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(300))
				.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(300)).build();
	}

}
