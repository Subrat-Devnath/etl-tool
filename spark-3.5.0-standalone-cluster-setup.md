# Apache Spark 3.5.0 Standalone Cluster Setup (Linux VM)

> **Scope:** Single-node Spark standalone cluster on a Linux VM, with a path to run a
> Java ETL JAR (e.g. `etl.jar`) against it. Start with one VM acting as both Master
> and Worker; later this same setup can be expanded to a real master/worker topology
> across multiple VMs.

## Overview

1. Verify/install Java
2. Download and install Spark 3.5.0
3. Configure environment variables
4. Configure Spark (`spark-env.sh`, `spark-defaults.conf`)
5. Start Spark Master
6. Start Spark Worker
7. Smoke-test with the built-in `SparkPi` example
8. Submit the Java ETL JAR
9. Size the configuration to the actual VM (CPU/RAM)

---

## 1. Check Java

Spark 3.5.0 supports Java 8, 11, and 17. For a Java-based ETL project, **Java 17** is
the recommended choice.

```bash
java -version
```

If Java 17 is not installed:

```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

Verify:

```bash
java -version
```

---

## 2. Download Spark 3.5.0

```bash
cd /opt
sudo wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-without-hadoop.tgz
```

Extract:

```bash
sudo tar -xzf spark-3.5.0-bin-without-hadoop.tgz
```

Rename for convenience:

```bash
sudo mv spark-3.5.0-bin-without-hadoop spark
```

Fix ownership so the current user can manage it without `sudo`:

```bash
sudo chown -R $USER:$USER /opt/spark
```

---

## 3. Environment variables

```bash
nano ~/.bashrc
```

Add the following lines:

```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Reload the shell config:

```bash
source ~/.bashrc
```

Verify Spark is on the `PATH` and the version is correct:

```bash
spark-submit --version
```

Expected output includes:

```
version 3.5.0
```

---

## 4. Spark configuration files

```bash
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh
cp spark-defaults.conf.template spark-defaults.conf
```

### 4.1 `spark-env.sh`

```bash
nano spark-env.sh
```

Example for a VM with **4 CPU / 8 GB RAM**:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

export SPARK_MASTER_HOST=<VM_PRIVATE_IP>
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

export SPARK_WORKER_CORES=3
export SPARK_WORKER_MEMORY=6G
export SPARK_WORKER_WEBUI_PORT=8081
```

Replace `<VM_PRIVATE_IP>` with the VM's actual private IP. Find it with:

```bash
hostname -I
```

> ⚠️ These numbers (3 cores / 6G) assume **8 GB RAM**. See [section 9](#9-size-the-configuration-to-your-actual-vm) before using this as-is — on a smaller VM this will starve the OS and crash Spark.

### 4.2 `spark-defaults.conf`

```bash
nano spark-defaults.conf
```

```properties
spark.master                                  spark://<VM_PRIVATE_IP>:7077
spark.driver.memory                           2g
spark.executor.memory                         2g
spark.executor.cores                          2
spark.sql.shuffle.partitions                  8
spark.default.parallelism                     8
spark.serializer                              org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.enabled                    true
spark.sql.adaptive.coalescePartitions.enabled true
spark.network.timeout                         300s
spark.executor.heartbeatInterval              30s
```

Again, replace `<VM_PRIVATE_IP>` with the VM's actual private IP.

---

## 5. Start Spark Master

```bash
start-master.sh
```

Check the running JVM processes:

```bash
jps
```

You should see:

```
Master
```

Open the Spark Master UI in a browser:

```
http://<VM_IP>:8080
```

---

## 6. Start Spark Worker

On the **same VM**, register a worker against the master:

```bash
start-worker.sh spark://<VM_PRIVATE_IP>:7077
```

Check again:

```bash
jps
```

You should now see both:

```
Master
Worker
```

Refresh `http://<VM_IP>:8080` — the UI should now show something like:

```
Workers: 1
Worker ID      spark://...
Cores          3
Memory         6.0 GB
```

---

## 7. Smoke-test the cluster

Run the built-in example:

```bash
run-example SparkPi 10
```

Or submit it explicitly against the master:

```bash
spark-submit \
  --master spark://<VM_PRIVATE_IP>:7077 \
  $SPARK_HOME/examples/jars/spark-examples_2.12-3.5.0.jar \
  10
```

The exact example JAR filename can vary by build; confirm it first:

```bash
ls $SPARK_HOME/examples/jars/
```

---

## 8. Submit the Java ETL JAR

Assume the ETL JAR is named `etl.jar`.

Copy it to the VM:

```bash
mkdir -p ~/spark-apps
cp etl.jar ~/spark-apps/
```

### Client deploy mode

Driver runs on the machine that issues `spark-submit` (useful for interactive runs / easier log access):

```bash
spark-submit \
  --class com.example.etl.EtlApplication \
  --master spark://<VM_PRIVATE_IP>:7077 \
  --deploy-mode client \
  --executor-memory 2G \
  --executor-cores 2 \
  ~/spark-apps/etl.jar
```

### Cluster deploy mode

Driver runs inside the cluster instead (better for unattended/production jobs):

```bash
spark-submit \
  --class com.example.etl.EtlApplication \
  --master spark://<VM_PRIVATE_IP>:7077 \
  --deploy-mode cluster \
  --executor-memory 2G \
  --executor-cores 2 \
  ~/spark-apps/etl.jar
```

Replace `com.example.etl.EtlApplication` with the actual main class of the ETL JAR.

---

## 9. Size the configuration to your actual VM

The `spark-env.sh` / `spark-defaults.conf` values above are only an **example** for a
4 CPU / 8 GB VM. **Do not reuse them blindly on a smaller VM** — Spark, the OS, the JVM,
and the ETL job itself all compete for the same RAM; on a 4 GB VM (or less) this
configuration will likely cause OOM kills or make the VM unresponsive.

Before finalizing the configuration, collect the VM's actual specs:

```bash
nproc
free -h
df -h
java -version
```

Use these outputs to size, at minimum:

- `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` (in `spark-env.sh`)
- `spark.executor.memory` / `spark.executor.cores` / `spark.driver.memory` (in `spark-defaults.conf`)
- `--executor-memory` / `--executor-cores` on the `spark-submit` command for the ETL JAR

As a rule of thumb, reserve 1–2 GB of RAM and at least 1 CPU core for the OS and
Spark's own daemons before allocating the rest to the worker/executors.

---

## Quick reference: what to replace

| Placeholder | Where | How to get it |
|---|---|---|
| `<VM_PRIVATE_IP>` | `spark-env.sh`, `spark-defaults.conf`, `spark-submit --master` | `hostname -I` |
| `<VM_IP>` | Browser URL for Spark UI | Same as above (or public IP if accessing remotely) |
| `com.example.etl.EtlApplication` | `spark-submit --class` | Main class of the ETL JAR |
| `etl.jar` | `spark-submit` final argument | Actual ETL JAR filename |
