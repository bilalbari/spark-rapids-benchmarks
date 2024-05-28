# NDS-H v2.0 Automation

## Disclaimer

NDS-H is derived from the TPC-H Benchmarks and as such any results obtained using NDS-H are not
comparable to published TPC-H Benchmark results, as the results obtained from using NDS-G do not
comply with the TPC-H Benchmarks.

## License

NDS-H is licensed under Apache License, Version 2.0.

Additionally, certain files in NDS are licensed subject to the accompanying [TPC EULA](../TPC%20EULA.txt) (also
available at [tpc.org](http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). Files subject to the TPC
EULA are identified as such within the files.

You may not use NDS except in compliance with the Apache License, Version 2.0 and the TPC EULA.

## Prerequisites

1. python >= 3.6
2. Necessary libraries

    ```bash
    sudo locale-gen en_US.UTF-8
    sudo apt install openjdk-8-jdk-headless gcc make flex bison byacc maven
    ```

3. TPC-H Tools

    User must download TPC-H Tools from [official TPC website](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). The tool will be downloaded as a zip package with a random guid string prefix.
    After unzipping it, a folder called `TPC-H V3.0.1` will be seen.

    User must set a system environment variable `TPCH_HOME` pointing to this directory. e.g.

    ```bash
    export TPCH_HOME='/PATH/TO/YOUR/TPC-H V3.0.1'
    ```
    This variable will help find the TPC-H Tool when building essential component for this repository.

    User must set the variables `DSS_QUERY` to the folder queries inside `TPCH_HOME`/dbgen. This helps the query gen utility to find the templates

    ```bash
    export DSS_QUERY='/PATH/TO/YOUR/TPCH_HOME/dbgen/queries'
    ```

## Use spark-submit-template with template

To help user run NDS-H, we provide a template to define the main Spark configs for spark-submit command.
User can use different templates to run NDS with different configurations for different environment.
We create [spark-submit-template](./spark-submit-template), which accepts a template file and
submit the Spark job with the configs defined in the template file.

Example command to submit via `spark-submit-template` utility:

```bash
./spark-submit-template convert_submit_cpu.template \
ndsH_transcode.py  raw_sf3k  parquet_sf3k report.txt
```

We give 3 types of template files used in different steps of NDS:

1. convert_submit_*.template for converting the data by using ndsH_transcode.py
2. power_run_*.template for power run by using nds_power.py

We predefine different template files for different environment.
For example, we provide below template files to run ndsH_transcode.py for different environment:

* `convert_submit_cpu.template` is for Spark CPU cluster
* `convert_submit_cpu_delta.template` is for Spark CPU cluster with DeltaLake
* `convert_submit_cpu_iceberg.template` is for Spark CPU cluster with Iceberg
* `convert_submit_gpu.template` is for Spark GPU cluster

You need to choose one as your template file and modify it to fit your environment.
We define a [base.template](./base.template) to help you define some basic variables for your envionment.
And all the other templates will source `base.template` to get the basic variables.
When you hope to run multiple steps of NDS, you just need to modify `base.template` to fit for your cluster.

## Data Generation

### Build the jar for data generation

```bash
cd tpch-gen
make
```

### Generate data

How to generate data to local or HDFS

```bash
$ python ndsH_gen_data.py -h
usage: ndsH_gen_data.py [-h] [--overwrite_output] scale parallel data_dir
positional arguments:
  scale               volume of data to generate in GB.
  parallel            build data in <parallel_value> separate chunks
  data_dir            generate data in directory.

optional arguments:
  -h, --help          show this help message and exit
  --overwrite_output  overwrite if there has already existing data in the path provided
                      
```

Example command:

```bash
python nds_gen_data.py hdfs 100 100 /data/raw_sf100 --overwrite_output
```

### Convert DSV to Parquet or Other data sources

To do the data conversion, the `nds_transcode.py` need to be submitted as a Spark job. User can leverage
the [spark-submit-template](./spark-submit-template) utility to simplify the submission.
The utility requires a pre-defined [template file](./convert_submit_gpu.template) where user needs to put
necessary Spark configurations. Either user can submit the `nds_transcode.py` directly to spark with
arbitrary Spark parameters.

DSV ( pipe ) is the default input format for data conversion, it can be overridden by `--input_format`.

Parquet, Orc, Avro, JSON and Iceberg are supported for output data format at present with CPU. For GPU conversion,
only Parquet and Orc are supported.

## Query Generation

The [templates.patch](./tpcds-gen/patches/templates.patch) that contains necessary modifications to make NDS queries runnable in Spark will be applied automatically in the build step. The final query templates will be in folder `$TPCDS_HOME/query_templates` after the build process.

we applied the following changes to original templates released in TPC-DS v3.2.0:

* add `interval` keyword before all `date interval add` mark `+` for syntax compatibility in Spark SQL.

* convert `"` mark to `` ` `` mark for syntax compatibility in Spark SQL.

### Generate Specific Query or Query Streams

```text
usage: nds_gen_query_stream.py [-h] (--template TEMPLATE | --streams STREAMS)
                               template_dir scale output_dir

positional arguments:
  template_dir         directory to find query templates and dialect file.
  scale                assume a database of this scale factor.
  output_dir           generate query in directory.

optional arguments:
  -h, --help           show this help message and exit
  --template TEMPLATE  build queries from this template. Only used to generate one query from one tempalte. This argument is mutually exclusive with --streams. It
                       is often used for test purpose.
  --streams STREAMS    generate how many query streams. This argument is mutually exclusive with --template.
  --rngseed RNGSEED    seed the random generation seed.
```

Example command to generate one query using query1.tpl:

```bash
python nds_gen_query_stream.py $TPCDS_HOME/query_templates 3000 ./query_1 --template query1.tpl
```

Example command to generate 10 query streams each one of which contains all NDS queries but in
different order:

```bash
python nds_gen_query_stream.py $TPCDS_HOME/query_templates 3000 ./query_streams --streams 10
```

## Benchmark Runner

### Build Dependencies

There's a customized Spark listener used to track the Spark task status e.g. success or failed
or success with retry. The results will be recorded at the json summary files when all jobs are
finished. This is often used for test or query monitoring purpose.

To build:

```bash
cd jvm_listener
mvn package
```

`nds-benchmark-listener-1.0-SNAPSHOT.jar` will be generated in `jvm_listener/target` folder.

### Power Run

_After_ user generates query streams, Power Run can be executed using one of them by submitting `nds_power.py` to Spark.

Arguments supported by `nds_power.py`:

```text
usage: nds_power.py [-h] [--input_format {parquet,orc,avro,csv,json,iceberg,delta}] [--output_prefix OUTPUT_PREFIX] [--output_format OUTPUT_FORMAT] [--property_file PROPERTY_FILE] [--floats] [--json_summary_folder JSON_SUMMARY_FOLDER] [--delta_unmanaged] [--hive] input_prefix query_stream_file time_log

positional arguments:
  input_prefix          text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"). If input_format is "iceberg", this argument will be regarded as the value of property "spark.sql.catalog.spark_catalog.warehouse". Only default Spark catalog session name
                        "spark_catalog" is supported now, customized catalog is not yet supported. Note if this points to a Delta Lake table, the path must be absolute. Issue: https://github.com/delta-io/delta/issues/555
  query_stream_file     query stream file that contains NDS queries in specific order
  time_log              path to execution time log, only support local path.

optional arguments:
  -h, --help            show this help message and exit
  --input_format {parquet,orc,avro,csv,json,iceberg,delta}
                        type for input data source, e.g. parquet, orc, json, csv or iceberg, delta. Certain types are not fully supported by GPU reading, please refer to https://github.com/NVIDIA/spark-rapids/blob/branch-22.08/docs/compatibility.md for more details.
  --output_prefix OUTPUT_PREFIX
                        text to prepend to every output file (e.g., "hdfs:///ds-parquet")
  --output_format OUTPUT_FORMAT
                        type of query output
  --property_file PROPERTY_FILE
                        property file for Spark configuration.
  --floats              When loading Text files like json and csv, schemas are required to determine if certain parts of the data are read as decimal type or not. If specified, float data will be used.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        Empty folder/path (will create if not exist) to save JSON summary file for each query.
  --delta_unmanaged     Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without leveraging a
                        Metastore service
  --hive                use table meta information in Hive metastore directly without registering temp views.
  --extra_time_log EXTRA_TIME_LOG
                        extra path to save time log when running in cloud environment where driver node/pod cannot be accessed easily. User needs to add essential extra jars and configurations to access different cloud storage systems. e.g. s3, gs etc.
  --sub_queries SUB_QUERIES
                        comma separated list of queries to run. If not specified, all queries in the stream file will be run. e.g. "query1,query2,query3". Note, use "_part1" and "_part2" suffix for the following query names: query14, query23, query24, query39. e.g. query14_part1,
                        query39_part2
```

Example command to submit nds_power.py by spark-submit-template utility:

```bash
./spark-submit-template power_run_gpu.template \
nds_power.py \
parquet_sf3k \
./nds_query_streams/query_0.sql \
time.csv \
--property_file properties/aqe-on.properties
```

User can also use `spark-submit` to submit `nds_power.py` directly.

To simplify the performance analysis process, the script will create a local CSV file to save query(including TempView creation) and corresponding execution time. Note: please use `client` mode(set in your `power_run_gpu.template` file) when running in Yarn distributed environment to make sure the time log is saved correctly in your local path.

Note the template file must follow the `spark-submit-template` utility as the _first_ argument.
All Spark configuration words (such as `--conf` and corresponding `k=v` values)  are quoted by
double quotes in the template file. Please follow the format in [power_run_gpu.template](./power_run_gpu.template).

User can define the `properties` file like [aqe-on.properties](./properties/aqe-on.properties). The properties will be passed to the submitted Spark job along with the configurations defined in the template file. User can define some common properties in the template file and put some other properties that usually varies in the property file.

The command above will use `collect()` action to trigger Spark job for each query. It is also supported to save query output to some place for further verification. User can also specify output format e.g. csv, parquet or orc:

```bash
./spark-submit-template power_run_gpu.template \
nds_power.py \
parquet_sf3k \
./nds_query_streams/query_0.sql \
time.csv \
--output_prefix /data/query_output \
--output_format parquet
```

### Throughput Run

Throughput Run simulates the scenario that multiple query sessions are running simultaneously in
Spark.

We provide an executable bash utility `nds-throughput` to do Throughput Run.

Example command for Throughput Run that runs _2_ Power Run in parallel with stream file _query_1.sql_
and _query_2.sql_ and produces csv log for execution time _time_1.csv_ and _time_2.csv_.

```bash
./nds-throughput 1,2 \
./spark-submit-template power_run_gpu.template \
nds_power.py \
parquet_sf3k \
./nds_query_streams/query_'{}'.sql \
time_'{}'.csv
```

When providing `spark-submit-template` to Throughput Run, please do consider the computing resources
in your environment to make sure all Spark job can get necessary resources to run at the same time,
otherwise some query application may be in _WAITING_ status(which can be observed from Spark UI or
Yarn Resource Manager UI) until enough resources are released.

### Data Maintenance

Data Maintenance performance data update over existed dataset including data INSERT and DELETE. The
update operations cannot be done atomically on raw Parquet/Orc files, so we use
[Iceberg](https://iceberg.apache.org/) as dataset metadata manager to overcome the issue.

Enabling Iceberg requires additional configuration. Please refer to [Iceberg Spark](https://iceberg.apache.org/docs/latest/getting-started/)
for details. We also provide a Spark submit template with necessary Iceberg configs: [maintenance_iceberg.template](./maintenance_iceberg.template)

The data maintenance queries are in [data_maintenance](./data_maintenance) folder. `DF_*.sql` are
DELETE queries while `LF_*.sql` are INSERT queries.

Note: The Delete functions in Data Maintenance cannot run successfully in Spark 3.2.0 and 3.2.1 due
to a known Spark [issue](https://issues.apache.org/jira/browse/SPARK-39454). User can run it in Spark 3.2.2
or later. More details including work-around for version 3.2.0 and 3.2.1 could be found in this
[link](https://github.com/NVIDIA/spark-rapids-benchmarks/pull/9#issuecomment-1141956487)

Arguments supported for data maintenance:

```text
usage: nds_maintenance.py [-h] [--maintenance_queries MAINTENANCE_QUERIES] [--property_file PROPERTY_FILE] [--json_summary_folder JSON_SUMMARY_FOLDER] [--warehouse_type {iceberg,delta}] [--delta_unmanaged] warehouse_path refresh_data_path maintenance_queries_folder time_log

positional arguments:
  warehouse_path        warehouse path for Data Maintenance test.
  refresh_data_path     path to refresh data
  maintenance_queries_folder
                        folder contains all NDS Data Maintenance queries. If "--maintenance_queries"
                        is not set, all queries under the folder will beexecuted.
  time_log              path to execution time log in csv format, only support local path.

optional arguments:
  -h, --help            show this help message and exit
  --maintenance_queries MAINTENANCE_QUERIES
                        specify Data Maintenance query names by a comma seprated string. e.g. "LF_CR,LF_CS"
  --property_file PROPERTY_FILE
                        property file for Spark configuration.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        Empty folder/path (will create if not exist) to save JSON summary file for each query.
  --warehouse_type {iceberg,delta}
                        Type of the warehouse used for Data Maintenance test.
  --delta_unmanaged     Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without leveraging a Metastore service.
```

An example command to run only _LF_CS_ and _DF_CS_ functions:

```bash
./spark-submit-template maintenance_iceberg.template \
nds_maintenance.py \
update_data_sf3k \
./data_maintenance \
time.csv \
--maintenance_queries LF_CS,DF_CS \
--data_format orc
```

Note: to make the maintenance query compatible in Spark, we made the following changes:

1. change `CREATE VIEW` to `CREATE TEMP VIEW` in all INSERT queries due to [[SPARK-29630]](https://github.com/apache/spark/pull/26361)
2. change data type for column `sret_ticket_number` in table `s_store_returns` from `char(20)` to `bigint` due to [known issue](https://github.com/NVIDIA/spark-rapids-benchmarks/pull/9#issuecomment-1138379596)

## Data Validation

To validate query output between Power Runs with and without GPU, we provide [nds_validate.py](nds_validate.py)
to do the job.

Arguments supported by `nds_validate.py`:

```text
usage: nds_validate.py [-h] [--input1_format INPUT1_FORMAT] [--input2_format INPUT2_FORMAT] [--max_errors MAX_ERRORS] [--epsilon EPSILON] [--ignore_ordering] [--use_iterator] [--floats] --json_summary_folder JSON_SUMMARY_FOLDER input1 input2 query_stream_file

positional arguments:
  input1                path of the first input data.
  input2                path of the second input data.
  query_stream_file     query stream file that contains NDS queries in specific order.

optional arguments:
  -h, --help            show this help message and exit
  --input1_format INPUT1_FORMAT
                        data source type for the first input data. e.g. parquet, orc. Default is: parquet.
  --input2_format INPUT2_FORMAT
                        data source type for the second input data. e.g. parquet, orc. Default is: parquet.
  --max_errors MAX_ERRORS
                        Maximum number of differences to report.
  --epsilon EPSILON     Allow for differences in precision when comparing floating point values.
                        Given 2 float numbers: 0.000001 and 0.000000, the diff of them is 0.000001 which is less than the epsilon 0.00001, so we regard this as acceptable and will not report a mismatch.
  --ignore_ordering     Sort the data collected from the DataFrames before comparing them.
  --use_iterator        When set, use `toLocalIterator` to load one partition at a time into driver memory, reducing.
                        memory usage at the cost of performance because processing will be single-threaded.
  --floats              whether the input data contains float data or decimal data. There're some known mismatch issues due to float point, we will do some special checks when the input data is float for some queries.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        path of a folder that contains json summary file for each query.

```

Example command to compare output data of two queries:

```bash
python nds_validate.py \
query_output_cpu \
query_output_gpu \
./nds_query_streams/query_1.sql \
--ignore_ordering
```

## Whole Process NDS Benchmark

[nds_bench.py](./nds_bench.py) along with its yaml config file [bench.yml](./bench.yml) is the script
to run the whole process NDS benchmark to get final metrics.
User needs to fill in the config file to specify the parameters of the benchmark.
User can specify the `skip` field in the config file to skip certain part of the benchmarks.
Please note: each part of the benchmark will produce its report file for necessary metrics like total
execution time, start or end timestamp. The final metrics are calculated by those reports. Skipping
a part of the benchmark may cause metrics calculation failure in the end if there's no necessary reports
generated previously.

Example command to run the benchmark:

```text
usage: python nds_bench.py [-h] yaml_config

positional arguments:
  yaml_config  yaml config file for the benchmark
```

NOTE: For Throughput Run, user should create a new template file based on the one used for Power Run.
The only difference between them is that the template for Throughput Run should limit the compute resource
based on the number of streams used in the Throughput Run.
For instance: 4 concurrent streams in one Throughput run and the total available cores in the benchmark cluster
are 1024. Then in the template, `spark.cores.max` should be set to `1024/4=256` so that each stream will have
compute resource evenly.

### NDS2.0 is using source code from TPC-DS Tool V3.2.0
