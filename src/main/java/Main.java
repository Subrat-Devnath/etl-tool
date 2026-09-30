import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;

public class Main {

    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(Main.class.getName());

    static final Map<String, String> filesystems = new LinkedHashMap<>();

    static {
        filesystems.put("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        filesystems.put("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        filesystems.put("fs.ftp.impl", org.apache.hadoop.fs.ftp.FTPFileSystem.class.getName());
    }

    public static void main(String[] args) throws Exception {
        printBanner();
        SparkSession spark = createSparkSession();
        initFileSystems(spark);
        executeJob(spark, getJobClass(args));
    }

    public static void printBanner() {
        try (InputStream in = Main.class.getClassLoader().getResourceAsStream("banner.txt")) {
            if (in == null) {
                log.warn("banner.txt not found on classpath");
                return;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                System.out.println(reader.lines().collect(Collectors.joining(System.lineSeparator())));
            }
        } catch (Exception e) {
            log.warn("Failed to print banner", e);
        }
    }

    public static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .master(master())
                .getOrCreate();
    }

    public static String master() {
        String master = System.getProperty("spark.master");
        if (master == null) {
            log.info("Remote spark cluster not found!!! Using standalone");
            master = "local[*]";
        }
        return master;
    }

    public static void initFileSystems(SparkSession spark) {
        filesystems.forEach((key, value) -> {
            spark.conf().set(key, value);
            spark.sparkContext().hadoopConfiguration().set(key, value);
        });
    }

    public static Class<?> getJobClass(String[] args) throws ClassNotFoundException {
        String mainClass = args.length == 0 ? "etl.ETLJob" : args[0];
        return Class.forName(mainClass);
    }

    public static void executeJob(SparkSession spark, Class<?> job) throws Exception {
        Constructor<?> constructor = job.getConstructors()[0];
        Object etl = constructor.newInstance(spark);
		// Calling run method of ETLJob class
        job.getMethod("run").invoke(etl);
    }

}
