# ETL Job

This is the Java port of the Scala `etl-tool` project. Job yaml files, system properties and
the `spark-submit` entry point are unchanged, so existing jobs run as-is.

## Build

```bash
mvn clean package
```

That produces the fat jar `target/etl_tool-assembly-<version>.jar`, replacing what `sbt assembly`
used to build. The main class is `Main` and the default job class is `etl.ETLJob`.

## Run

```bash
spark-submit --class Main \
  --conf spark.driver.extraJavaOptions="-Djob_mode=yaml -Djob=myjob" \
  target/etl_tool-assembly-0.0.1.jar
```

Running it directly on Java 17 needs the usual Spark `--add-opens` flags, for example
`--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED`.

Supported system properties are unchanged: `job_mode` (`yaml` or `cassandra`), `job`,
`spark.master`, and for cassandra mode `cql_host`, `cql_port`, `cql_user`, `cql_pass`,
`cql_keyspace`, `cql_data_center`, `cql_consistency`.

## If you introduce any column in a UDT field, you need to create a codec to convert the UDT data into the corresponding UDT object.
Examples can be found in the following method:  
`etl.model.transform.TransformCodec`

DataStax Document For Codec: https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/custom_codecs/index.html#creating-custom-java-to-cql-mappings-with-mapping-codec

---
## v4 - Introduced sort options
```yaml
sort: 
    col: String
    order: asc, asc_nulls_first, asc_nulls_last, desc, desc_nulls_first, desc_nulls_last
```

---
## v3 - Skip error flag
Added a flag skipErrors in jobGroup to be able to skip errors for multiple chained jobs
```yaml
skipErrors: true
jobs:
  - name: job1
    extract:
    transform:
    load:
  - name: job2
    extract:
    transform:
    load:
```


---
## v2 - Random ID udf
```yaml
      - add:
          - col: String
          - udf:
            function: id
```

---
## v1 - Branch Strategy
We will start to migrate to newer versions of spark libraries as and when possible, as we are unable to get hotfixes from spark library (e.g. recent issue of OOM, CSV unable to handle blank files, etc). 

To be able to do this smoothly, I have modified the ETL tool branching strategy to move from gitflow to trunk based... 

I recommend to use latest spark library for writing new jobs, and not change existing jobs, unless you have bandwidth to upgrade and test... (execution may drastically change with each spark release)

here are things that will be managed differently from regular product release strategy...

1. trunk branch to contain stable commits from developers
2. release branches created for each supported spark library version, e.g. 2.4.0, 2.4.5 (3.0 may come in future)
3. artifacts (jar file) created will have spark library version attached to it, e.g. etl_tool-assembly-2.4.0.1.jar, etl_tool-assembly-2.4.5.1.jar
4. all the fixes/changes will be available in all spark versions (unless that feature is not supported by spark itself)
5. Readme will contain version reference of minor version of release (e.g. for a feature released in 2.4.0.1, 2.4.5.1, readme will have reference of v1)

Note: The ETL releases are not covered by Unit Test/Manual Test/Automated Tests. Modify the tool version carefully after thorough testing...

---
## v 0.9.42

added support to read JSON String column having root level as an Array
```yaml
fromsjon:
  col: <String> name of column
  rootArray: true
```
---
## v 0.9.40

added transformation to convert JSON String column to Struct
```yaml
fromsjon:
  col: <String> name of column
```
---
## v 0.9.37

added status update callback REST api
```yaml
conf:
  status.callback:
    api: http://abc.xyz/please_update_status
    method: POST
    params:
      p1: v1
      p2: v2
    headers:
      authorization: Bearer auth_token
```

---
## v 0.9.32

added xml reader
```yaml
- xml:
      col: <String> name of column
```
Optional - add target dataset otherwise it will override current dataset.

---
## v 0.9.31

UDF to split column values
```yaml
- add:
      col: <String> name of column
      udf: 
        function: split
        col: column to split
        params:
          - <split char>
          - <position of splitted array to extract>
```
---


## v 0.9.25

Support to get count of rows in dataset
```yaml
- count:
      col: <String> name of column that will contain the count value
```
The dataset will be reset and contain only one row with one column, i.e. count. Advise to use a target so that main dataset is not lost.

---


## v 0.9.22

Support for filter expressions

```yaml
- filterExpression:
      expression: <condition>
```
expressions are provided in https://spark.apache.org/docs/2.3.0/api/sql/index.html

---


## v 0.8.x

Implementation of join types

```yaml
- join:
      right: dataset
      col:
        - col1
        - coln
      joinType: inner|cross|outer|full|full_outer|left|left_outer|right|right_outer|left_semi|left_anti (default - inner)
```

---


## v 0.7.x

CICD implementation

---
## v 0.6.x

column to JSON impl as transform task

```yaml
- json:
      col:
        - col1
        - coln
```

added support to add nested col directly into existing struct (1 level is supported in this version)

0.6.1 - Fixed json parsing issue

---

## v 0.5.x

logger implementation to output etl.log on the path where tool is run,

the file will concat to previous log

the file will rotate every 10 MB

---

## v 0.4.x

Impl coalesce option in load task
- default is one file
- enter 0 for output to no of files equal to no of partitions required by spark
- enter non-zero, positive number to output to fixed no of files (note: max no of files generated will be less than no of partitions required)

---

## v 0.3.x

Multi-col support in join condition

```yaml
- join:
      col:
        - col1
        - coln
      right: dataset
```

---

## v 0.2.x

added UDFs,

```yaml
value: 1560124799000
udf:
    function: date
    params:
      - timestamp

value: 05/10/2019
udf:
    function: date
    params:
      - MM/dd/yyyy

udf:
    function: dateToString
    col:
      - timestampDate
    params:
      - yyyy MMM dd

udf:
    function: seqToString
    col:
      - column of type list or map
    params:
      - ", "

udf:
    function: structToString
      - column of type struct
    params:
      - ","
```

---

## v 0.1.9

added filter by col
```yaml
- filter:
  col: columnName
  op: operator
  ref: another columnName

- filter:
  col: columnName
  op: operator
  value: value to filter

```

---

## v 0.1.8

added Delete mode support (only for cassandra) in load task

- Append
- Overwrite
- ErrorIfExists
- Ignore
- Delete

---

## v 0.1.7

impl new udf

- int
- boolean
- long

---

## v 0.1.6

Added aggregate functions for grouping

- avg
- collect_list
- collect_set
- count
- countDistinct
- first
- last
- max
- min
- sum

Added debug support

```yaml
debug:
  action: data|schema|count
```

---

## v 0.1.5

expression support - add columns with conditional expressions
e.g. 

```yaml
add:  
  col: String #name of column to filter  
  condition: String # conditional expression 
  value: String # value of col if condition is true (optional - if not provided, true/false will be output)
```

expressions are provided in https://spark.apache.org/docs/2.3.0/api/sql/index.html

---

## v 0.1.4

docker cluster impl 

install docker-ce or docker-desktop on your OS

https://docs.docker.com/toolbox/toolbox_install_windows/

OR 

https://www.docker.com/products/docker-desktop

OR

https://docs.docker.com/install/linux/docker-ce/ubuntu/

xxxxx -> spark master e.g. spark://int-spark.yagnaiq.com:7077

yyyyy -> your IP address


---

## v 0.1.3
downgraded cassandra connector to v2.3.0 due to bug in https://datastax-oss.atlassian.net/browse/SPARKC-541

---

## v 0.1.2

fixed: constant value not working in add transformation.

---

## v 0.1.1
added support for filter operators

```yaml
filter:  
  col: String #name of column to filter  
  op: String  # operator
  inverse: Boolean # if inversion is needed
  value: String # value of filter if applicable
```

Following operators are supported,

- NULL
- EQ
- GT
- LT
- GEQ
- LEQ
- NAN
- LIKE
- RLIKE - Regex
- STARTS
- ENDS

To define NULL, please use double quotes, as yaml parsing will assume it as actual null otherwise
https://yaml.org/type/null.html

---

## v 0.1 

Following documentation will help you build a job configuration for ETL processing

The job defination is written in yaml format, you can read here more about it - https://yaml.org/spec/1.2/spec.html

### Following elements are supported to define a job,

```yaml
jobs:
  - name: String
  - conf: Map[String, String]

  - extract:
      - format: String (jdbc, csv, com.crealytics.spark.excel, org.apache.spark.sql.cassandra)
      - all: Boolean (in case of csv and excel, whether to read all files in given directory)
      - options: Map[String, String]

  - transform:
      - select:
          - col : Array[String] - Columns to select

      - filter:
          - col: String - Column to filter
          - operator: String (NotNull, NotEmpty)

      - cast:
          - col: String
          - format: (double, date, dateTimestamp)

      - add:
          - col: String

          - value: Any (Constant value)
          OR
          - udf:
            function: String (now, nowTimestamp, coterm, uuid)
          OR
          - ref: String (duplicate column data from)

      - rename:
          - col: String
          - to: String (new column name)

      - drop:
          - col: String

  - load:
      - format: String (jdbc, csv, com.crealytics.spark.excel, org.apache.spark.sql.cassandra, console)
      - mode: String (Append, Overwrite, ErrorIfExists, Ignore)
      - options: Map[String, String]

```


For support contact @subrat.devnath

Command for run ETL:   

 for 1.8:
 
java -Djob_mode=yaml -Djob=job -jar etl_tool-assembly-0.0.1.jar


for 11 and 17:

java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -Djob_mode=yaml -Djob=job -jar etl_tool-assembly-0.0.1.jar

