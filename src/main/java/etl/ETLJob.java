package etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import base.ETL;
import etl.model.Job;
import etl.model.JobGroup;
import etl.provider.Provider;
import etl.provider.ProviderFactory;
import sidecar.Logger;

public class ETLJob extends ETL implements Logger {

    private final Provider provider = ProviderFactory.of(System.getProperty("job_mode"));

    public ETLJob(SparkSession spark) {
        super(spark);
    }

    @Override
    public void run() {
        UDF.registerUDF(spark);
        JobGroup jobGroup = provider.load();

        for (Job job : jobGroup.jobs) {
            processJob(job, jobGroup.skipErrors);
        }
    }

    public void processJob(Job job, boolean skipErrors) {
        log().info("Start processing job " + job.name);

        try {
            provider.before(job);

            configure(job);
            provider.metadata(spark, job);
            Map<String, Dataset<Row>> dataset = initDataset();

            extract(job, dataset);
            transform(job, dataset);
            load(job, dataset);

            log().info("Done processing job " + job.name);
            provider.after(job);

        } catch (Exception e) {
            provider.error(job);
            if (skipErrors) {
                log().error("Error while executing job - " + job.name, e);
            } else {
                throw e;
            }
        } finally {
            provider.end(job);
        }
    }

    public Map<String, Dataset<Row>> initDataset() {
        Map<String, Dataset<Row>> dataset = new HashMap<>();
        dataset.put("data", spark.createDataFrame(new ArrayList<Row>(), new StructType()));
        return dataset;
    }

    public void extract(Job job, Map<String, Dataset<Row>> dataset) {
        if (job.extract != null) {
            job.extract.forEach(extract -> extract.execute(job, spark, dataset));
        }
    }

    public void transform(Job job, Map<String, Dataset<Row>> dataset) {
        if (job.transform != null) {
            job.transform.forEach(transform -> transform.execute(job, spark, dataset));
        }
    }

    public void load(Job job, Map<String, Dataset<Row>> dataset) {
        if (job.load != null) {
            job.load.forEach(load -> load.execute(job, spark, dataset));
        }
    }

    public void configure(Job job) {
        if (job.conf == null) {
            return;
        }

        job.conf.forEach((key, value) -> {
            if (value instanceof String) {
                spark.conf().set(key, (String) value);
            }
            if (value instanceof Integer) {
                spark.conf().set(key, value.toString());
            }
        });
    }

}
