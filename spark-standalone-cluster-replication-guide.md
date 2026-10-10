# Replicating the Spark Standalone Setup on a New VM

> **Purpose:** Step-by-step guide to set up a new VM exactly the way Spark is configured
> on the existing `dev` VM pattern — same distribution, same directory layout, same
> start/stop scripts, same `spark-defaults.conf`, same cleanup automation. No VM-specific
> values (IPs, hostnames, disk/RAM numbers) are baked in here — replace the placeholders
> for each new VM.

## Placeholders used below

| Placeholder | What it means |
|---|---|
| `<VM_PRIVATE_IP>` | The new VM's private IP (`hostname -I`) |
| `<JAVA_8_HOME>` | Path to a Java 8 JDK on the new VM (see §1) |
| `<ETL_JAR_NAME>` | Filename of the Java ETL assembly jar you'll deploy |

---

## 0. Pre-flight checklist

Before installing anything, collect these on the new VM:

```bash
nproc
free -h
df -h
java -version
```

- Make sure there's enough **disk headroom** for Spark (~400 MB) + Hadoop (~600 MB) +
  the ETL jar (can be several hundred MB) + logs/work dirs that grow during job runs.
- Decide how much CPU/RAM you're willing to dedicate to the Spark worker vs. leaving for
  the OS and any other co-located services (Kafka, app servers, etc.) on the same box.

## 1. Java 8

This setup pins Spark specifically to **Java 8** (not 11/17), even if other JDKs are
also installed on the box for other services.

```bash
java -version
```

If Java 8 isn't installed:

```bash
sudo apt update
sudo apt install openjdk-8-jdk -y
```

Find the exact install path to use as `<JAVA_8_HOME>`:

```bash
ls /usr/lib/jvm/
```

It will look something like `/usr/lib/jvm/java-8-openjdk-amd64` or
`/usr/lib/jvm/jdk1.8.0_<build>` depending on how it was installed — use whatever path
actually exists on this VM, don't assume it matches another VM's path.

## 2. Install Spark (the "without-hadoop" build) + a companion Hadoop

This setup uses `spark-3.5.0-bin-without-hadoop` (Hadoop classes are *not* bundled) plus
a separately-installed Hadoop distribution purely to supply the classpath — there is
**no HDFS** involved; local filesystem only.

```bash
sudo mkdir -p /opt/spark
cd /opt/spark

# Spark (without-hadoop build)
sudo wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-without-hadoop.tgz
sudo tar -xzf spark-3.5.0-bin-without-hadoop.tgz
sudo rm spark-3.5.0-bin-without-hadoop.tgz

# Hadoop (classpath only, matching the version Spark 3.5.0 was built against)
sudo wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
sudo tar -xzf hadoop-3.3.4.tar.gz
sudo rm hadoop-3.3.4.tar.gz

sudo chown -R $USER:$USER /opt/spark
```

Resulting layout:

```
/opt/spark/
├── spark-3.5.0-bin-without-hadoop/
├── hadoop-3.3.4/
├── start-spark.sh      (created in §4)
└── stop-spark.sh       (created in §4)
```

Leave Hadoop's `etc/hadoop/core-site.xml` and `hdfs-site.xml` at their default (empty
`<configuration></configuration>`) — Hadoop is not being used as a filesystem here.

## 3. `spark-defaults.conf`

```bash
cd /opt/spark/spark-3.5.0-bin-without-hadoop/conf
cp spark-defaults.conf.template spark-defaults.conf
nano spark-defaults.conf
```

Add:

```properties
spark.master.rest.enabled                     true
spark.eventLog.enabled                         true
spark.eventLog.dir                             file:///tmp/spark-events
spark.worker.cleanup.enabled                   true
spark.worker.cleanup.interval                  3600
spark.worker.cleanup.appDataTtl                3600
spark.worker.ui.compressedLogFileLengthCacheSize 100
```

> ⚠️ **Fix applied here vs. the reference VM:** the reference setup enables
> `spark.eventLog.enabled` **without** setting `spark.eventLog.dir`, and without the
> target directory existing — which can break event logging. Create the directory and
> set the dir explicitly:
>
> ```bash
> sudo mkdir -p /tmp/spark-events && sudo chmod 1777 /tmp/spark-events
> ```

`spark.master`, `spark.driver.memory`, `spark.executor.memory`/`cores`, and the adaptive-
execution flags are intentionally **not** set here — they're passed per job on the
`spark-submit` command line instead (see §7).

## 4. `spark-env.sh` — reserve headroom for the worker

```bash
cd /opt/spark/spark-3.5.0-bin-without-hadoop/conf
cp spark-env.sh.template spark-env.sh
nano spark-env.sh
```

```bash
export JAVA_HOME=<JAVA_8_HOME>
export SPARK_WORKER_CORES=<cores to dedicate, e.g. nproc - 2>
export SPARK_WORKER_MEMORY=<memory to dedicate, e.g. total RAM - 2G>
```

> ⚠️ **Fix applied here vs. the reference VM:** the reference setup leaves
> `spark-env.sh` empty, so the worker defaults to claiming *all* CPU cores and nearly all
> RAM on the box. Explicitly reserve some for the OS and any other co-located services
> (Kafka, app servers, etc.) instead of leaving this unset.

`conf/workers` can stay empty if this VM will run both Master and Worker alone (the
pattern used here) — add one hostname/IP per line only if you're adding *additional*
worker VMs later.

## 5. `start-spark.sh` / `stop-spark.sh`

Create these in `/opt/spark/` (same level as the Spark and Hadoop directories, not
inside either of them):

```bash
sudo tee /opt/spark/start-spark.sh > /dev/null << 'EOF'
#!/bin/bash

cd
export HADOOP_HOME=/opt/spark/hadoop-3.3.4
export PATH=$PATH:$HADOOP_HOME/bin
export JAVA_HOME=<JAVA_8_HOME>

cd /opt/spark/spark-3.5.0-bin-without-hadoop/sbin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
./start-master.sh --host <VM_PRIVATE_IP>
sleep 5
./start-worker.sh spark://<VM_PRIVATE_IP>:7077
EOF

sudo tee /opt/spark/stop-spark.sh > /dev/null << 'EOF'
#!/bin/bash

cd
export HADOOP_HOME=/opt/spark/hadoop-3.3.4
export PATH=$PATH:$HADOOP_HOME/bin
export JAVA_HOME=<JAVA_8_HOME>
cd /opt/spark/spark-3.5.0-bin-without-hadoop/sbin
./stop-worker.sh
sleep 5
./stop-master.sh
EOF

sudo chmod +x /opt/spark/start-spark.sh /opt/spark/stop-spark.sh
```

> Note: the reference setup uses the legacy `start-slave.sh` / `stop-slave.sh` names.
> `start-worker.sh` / `stop-worker.sh` are the current (non-deprecated) equivalents in
> Spark 3.5.0 — used here instead; both work identically.

Why `SPARK_DIST_CLASSPATH=$(hadoop classpath)` matters: the "without-hadoop" Spark build
has no Hadoop jars of its own, so without this line, Spark can fail to find
`org.apache.hadoop.fs.FileSystem` and similar classes at startup.

## 6. Start / stop / verify

```bash
sudo /opt/spark/start-spark.sh
```

Verify:

```bash
sudo jps                 # expect: Master, Worker
curl -s http://<VM_PRIVATE_IP>:8080 | grep -i "Workers"
```

Open the UI in a browser: `http://<VM_IP>:8080`

Stop:

```bash
sudo /opt/spark/stop-spark.sh
```

## 7. Deploy and submit the ETL jar

```bash
mkdir -p /opt/spark/spark-3.5.0-bin-without-hadoop/etlJar
cp <ETL_JAR_NAME> /opt/spark/spark-3.5.0-bin-without-hadoop/etlJar/
```

```bash
spark-submit \
  --class com.example.etl.EtlApplication \
  --master spark://<VM_PRIVATE_IP>:7077 \
  --deploy-mode client \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/spark/spark-3.5.0-bin-without-hadoop/etlJar/<ETL_JAR_NAME>
```

## 8. Automated cleanup of finished job folders (optional, but used on the reference VM)

Spark's own `spark.worker.cleanup.*` settings (§3) sweep worker app data after an hour,
but the reference setup also runs a tighter, faster cleanup script that removes a
job's `driver-*`/`app-*` folders ~60 seconds after it finishes — useful when disk space
is tight.

```bash
sudo tee /opt/clearSparkDriveApp.sh > /dev/null << 'EOF'
#!/bin/bash

WORK_DIR="/opt/spark/spark-3.5.0-bin-without-hadoop/work"

deleteByLog() {
    for DRIVER_PATH in "$WORK_DIR"/driver-*; do
        DRIVER_FOLDER=$(basename "$DRIVER_PATH")
        DRIVER_ID=${DRIVER_FOLDER#driver-}
        LAST_FOUR=${DRIVER_ID: -4}

        STDERR_LOG="$DRIVER_PATH/stderr"

        if [[ -f "$STDERR_LOG" ]]; then
            if grep -q 'status COMPLETED' "$STDERR_LOG"; then
                A="$DRIVER_PATH"
                B=$(find "$WORK_DIR" -maxdepth 1 -type d -name "app-*${LAST_FOUR}" | head -n1)
                (sleep 60 && rm -rf "$A" "$B") &
            fi

            if grep -q 'status FAILED' "$STDERR_LOG"; then
                A="$DRIVER_PATH"
                B=$(find "$WORK_DIR" -maxdepth 1 -type d -name "app-*${LAST_FOUR}" | head -n1)
                (sleep 60 && \
                    [[ -d "$B" ]] && rm -rf "$B"; \
                    find "$A" -maxdepth 1 -type f -name "*.jar" -exec rm -f {} \;; \
                    FOLDER_AGE=$(($(date +%s) - $(stat -c %Y "$A"))); \
                    if [[ $FOLDER_AGE -gt 3600 ]]; then rm -rf "$A"; fi
                ) &
            fi
        fi
    done
}

main(){
deleteByLog
echo "Delete spark jobs logs and jar cleanup done"
}

main
EOF

sudo chmod +x /opt/clearSparkDriveApp.sh
```

Schedule it (every 2 minutes, matching the reference VM):

```bash
(sudo crontab -l 2>/dev/null; echo "*/2 * * * * /opt/clearSparkDriveApp.sh") | sudo crontab -
```

## 9. (Optional) App-level env vars for ETL/Rundeck-style job orchestration

If the ETL jar is triggered by an external job runner (e.g. Rundeck) rather than invoked
by hand, the reference setup exposes these as plain env vars in `/etc/environment` for
the job runner / wrapper to read when building its `spark-submit` command:

```bash
SPARK_MASTER_HOST=<VM_PRIVATE_IP>
ETL_JAR_LOCATION=/opt/spark/spark-3.5.0-bin-without-hadoop/etlJar
SPARK_EXECUTOR_MEMORY=1g
SPARK_EXECUTOR_CORES=<set per job, don't leave as a placeholder like "NA">
SPARK_NUM_EXECUTORS=<set per job, don't leave as a placeholder like "NA">
SPARK_DRIVER_MEMORY=1g
SPARK_DRIVER_CORES=1
SPARK_DYNAMIC_ALLOCATION_ENABLED=false
SPARK_SCHEDULER_MODE=FIFO
```

> These are **not** native Spark environment variables that `spark-submit` reads
> automatically — they're app-level configuration for whatever wrapper/job-runner
> constructs the actual `spark-submit` invocation. Keep that wrapper consistent with
> whatever names it expects.

---

## Summary checklist for a new VM

- [ ] Confirm CPU/RAM/disk headroom (`nproc`, `free -h`, `df -h`)
- [ ] Java 8 installed, `<JAVA_8_HOME>` resolved
- [ ] `spark-3.5.0-bin-without-hadoop` + `hadoop-3.3.4` extracted under `/opt/spark/`
- [ ] `spark-defaults.conf` configured, **including `spark.eventLog.dir`** and the dir created
- [ ] `spark-env.sh` configured **with explicit `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY`** (don't leave it empty)
- [ ] `start-spark.sh` / `stop-spark.sh` created with this VM's IP and Java path
- [ ] `sudo /opt/spark/start-spark.sh` → `sudo jps` shows Master + Worker
- [ ] Master UI reachable at `http://<VM_IP>:8080`
- [ ] ETL jar copied to `etlJar/` and a test `spark-submit` succeeds
- [ ] (Optional) `clearSparkDriveApp.sh` + cron entry installed if disk space is a concern
