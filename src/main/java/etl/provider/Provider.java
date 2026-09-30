package etl.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.spark.sql.SparkSession;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import etl.model.Job;
import etl.model.JobGroup;
import etl.model.JobStatus;
import etl.model.ThirdPartyApplicationData;
import org.springframework.util.CollectionUtils;
import sidecar.Logger;

public abstract class Provider implements Logger {

	/**
	 * Row -&gt; POJO mapper shared by every provider, so subclasses don't have to hand-write a
	 * getXFromRow() method per entity (see CassandraProvider#getEtlJobFromRows previously) and
	 * don't need custom UDT TypeCodecs either - nested UDTs (Extract, Transform, Load, ...) are
	 * mapped automatically as long as they're annotated with {@code @UserDefinedType}.
	 * <p>
	 * It's built per-session (see {@link #initCassandraConverter(CqlSession)}) because UDT
	 * conversion needs a {@code UserTypeResolver} bound to a live {@code CqlSession} to look up
	 * the real UDT schema (field names/order) from the cluster.
	 * <p>
	 * The mapping context's default verifier is replaced with a no-op one because these model
	 * classes (Job, ThirdPartyApplicationData, ...) are read-only query projections, not full
	 * Cassandra @Table entities with a declared @PrimaryKey - the default verifier would
	 * otherwise reject them for having no id property.
	 */
	private CassandraConverter cassandraConverter;

	protected void initCassandraConverter(CqlSession cqlSession) {
		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(new SimpleUserTypeResolver(cqlSession));
		mappingContext.setVerifier(entity -> {
		});
		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);
		converter.setUserTypeResolver(new SimpleUserTypeResolver(cqlSession));
		converter.afterPropertiesSet();
		this.cassandraConverter = converter;
	}

	/**
	 * Converts every row of a ResultSet into an instance of {@code cls}, matching columns to
	 * fields/properties by name (via CassandraConverter) instead of a hand-written
	 * row.getX("column") per field. Call {@link #initCassandraConverter(CqlSession)} first.
	 */
	public List<?> getEntitiesFromResultSet(ResultSet resultSet, Class<?> cls) {
		List<Object> entityList = new ArrayList<>();

		for (Row row : resultSet.all()) {
			entityList.add(cassandraConverter.read(cls, row));
		}

		return entityList;
	}

	public abstract String mode();

	public abstract JobGroup load();

	public void metadata(SparkSession spark, Job job) {
	}

	public void before(Job job) {
		updateStatus(job, JobStatus.PROCESSING);
	}

	public void after(Job job) {
		updateStatus(job, JobStatus.COMPLETED);
	}

	public void error(Job job) {
		updateStatus(job, JobStatus.FAILED);
	}

	public void end(Job job) {
	}

	@SuppressWarnings("unchecked")
	public void updateStatus(Job job, JobStatus status) {
		log().info("Updating job " + job.name + " status " + status);

		try {
			if (job.conf.get("status.callback") != null) {
				updateStatusUsingCallback(job, status, (Map<String, Object>) job.conf.get("status.callback"));
			}
		} finally {
			log().info("Updated job " + job.name + " status " + status);
		}
	}

	@SuppressWarnings("unchecked")
	public void updateStatusUsingCallback(Job job, JobStatus status, Map<String, Object> statusCallback) {
		HttpPost post = new HttpPost((String) statusCallback.get("api"));

		List<BasicNameValuePair> params = new ArrayList<>();
		params.add(new BasicNameValuePair("status", status.toString()));

		((Map<String, String>) statusCallback.get("params")).forEach((key, value) -> {
			log().info("setting param " + key + " " + value);
			params.add(new BasicNameValuePair(key, value));
		});
		post.setEntity(new UrlEncodedFormEntity(params, java.nio.charset.StandardCharsets.UTF_8));

		((Map<String, String>) statusCallback.get("headers")).forEach(post::setHeader);

		try (CloseableHttpClient client = HttpClients.createDefault();
			 CloseableHttpResponse response = client.execute(post)) {
			log().info("Status update callback response " + response);
		} catch (Exception e) {
			log().error("Error updating status by api - " + e);
		}
	}

	@SuppressWarnings("unchecked")
	public void addS3BucketCred(String cql_keyspace, String environment, CqlSession cqlSession, SparkSession spark) {

		if (cql_keyspace == null || cql_keyspace.isEmpty()) {
			log().info("Nodes: " + cqlSession.getMetadata().getNodes() + " And Keyspace: " + cql_keyspace);
			return;
		}

		try {
			initCassandraConverter(cqlSession);

			ResultSet rs = cqlSession.execute("SELECT * FROM third_party_application_data WHERE organization_id='NA'"
					+ " and community_id='NA' and provider_name='AWS' and application_context='S3'");

			List<ThirdPartyApplicationData> dataList = (List<ThirdPartyApplicationData>)
					getEntitiesFromResultSet(rs, ThirdPartyApplicationData.class);

			if (CollectionUtils.isEmpty(dataList)) {
				return;
			}

			// first check if third party table entry present for given environment
			ThirdPartyApplicationData data = checkEnvironmentEntry(dataList, environment);

			if (data == null) {
				// if third party table entry present then check if third party table entry
				// present for environment = "NA"
				data = checkGenericEntry(dataList);
			}

			if (data != null && data.client_id != null && data.client_secret != null) {
				spark.conf().set("fs.s3a.access.key", data.client_id);
				spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", data.client_id);

				spark.conf().set("fs.s3a.secret.key", data.client_secret);
				spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", data.client_secret);
			} else {
				log().error("third_party_application_data not present or credentials are not proper environment: "
						+ environment);
			}

		} finally {
			if (cqlSession != null) {
				cqlSession.close();
			}
		}
	}

	// first check if third party table entry present for given environment
	private ThirdPartyApplicationData checkEnvironmentEntry(List<ThirdPartyApplicationData> dataList,
															String environment) {

		ThirdPartyApplicationData found = null;

		for (ThirdPartyApplicationData data : dataList) {
			if (data != null && data.environment != null && data.environment.equals(environment)) {
				found = data;
			}
		}
		return found;
	}

	// if third party table entry present then check if third party table entry
	// present for environment = "NA"
	private ThirdPartyApplicationData checkGenericEntry(List<ThirdPartyApplicationData> dataList) {

		ThirdPartyApplicationData found = null;

		for (ThirdPartyApplicationData data : dataList) {
			if (data != null && data.environment != null && data.environment.equals("NA")) {
				found = data;
			}
		}
		return found;
	}

}
