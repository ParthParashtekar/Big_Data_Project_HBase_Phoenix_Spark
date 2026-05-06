# Evaluating Apache Phoenix as a NoSQL-to-SQL Bridge for SMART Telemetry Data on HBase

This repository contains the code, cleaned datasets, logs, benchmark results, Spark profiling outputs, and configuration notes for a Big Data Technologies final project.

The project evaluates Apache Phoenix as a SQL abstraction layer over Apache HBase for large-scale SMART telemetry data from the Backblaze Q1 2024 dataset. The experiments measure bulk loading performance, CRUD behavior, query performance with and without secondary indexes, Spark-based data profiling, and system monitoring using Prometheus and Grafana.

---

## Project Summary

Apache HBase provides scalable NoSQL storage, but analytical querying on non-row-key attributes often requires expensive full-table scans. Apache Phoenix addresses this limitation by providing SQL querying and secondary indexing over HBase tables.

This project evaluates Phoenix over HBase using Backblaze SMART telemetry data scaled from 10K rows to 5M rows, with an additional 10M row stress test.

Main evaluation areas:

* HBase and Phoenix setup in pseudo-distributed mode
* ETL pipeline for Backblaze SMART telemetry data
* Phoenix schema and salted row-key design
* Bulk CSV loading through Phoenix `psql.py`
* CRUD operation benchmark
* Query benchmarking with and without secondary indexes
* Phoenix `EXPLAIN` plan comparison
* Spark-based data profiling
* Prometheus and Grafana monitoring
* 10M row stress-test behavior

---

## Dataset

The project uses the Backblaze Drive Stats dataset for Q1 2024.

Original dataset:

```text
data_Q1_2024.zip
```

The raw dataset is not included in this repository due to size. It can be downloaded from [Backblaze Drive Stats](https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2024.zip).

Selected fields:

* `date`
* `serial_number`
* `model`
* `capacity_bytes`
* `datacenter`
* `cluster_id`
* `failure`
* `smart_5_raw`
* `smart_9_raw`
* `smart_187_raw`
* `smart_188_raw`
* `smart_197_raw`
* `smart_198_raw`
* `smart_199_raw`

---

## System Environment

Experiments were conducted on a single-node Linux server.

| Component   | Configuration                                    |
| ----------- | ------------------------------------------------ |
| RAM         | 16 GB                                            |
| CPU         | Intel i3 / quad-core                             |
| Storage     | HDD, greater than 2 TB                           |
| HBase Mode  | Pseudo-distributed                               |
| Query Layer | Apache Phoenix                                   |
| Profiling   | Apache Spark local mode                          |
| Monitoring  | Prometheus, Grafana, JMX exporter, node exporter |

Important: this was not a true multi-node distributed deployment. All experiments were conducted on a single-node server using pseudo-distributed HBase.

---

## Phoenix Schema

```sql
CREATE TABLE smart_telemetry (
    salt_bucket      TINYINT NOT NULL,
    serial_number    VARCHAR NOT NULL,
    event_date       DATE NOT NULL,
    model            VARCHAR,
    capacity_bytes   BIGINT,
    datacenter       VARCHAR,
    cluster_id       VARCHAR,
    failure          TINYINT,
    smart_5_raw      BIGINT,
    smart_9_raw      BIGINT,
    smart_187_raw    BIGINT,
    smart_188_raw    BIGINT,
    smart_197_raw    BIGINT,
    smart_198_raw    BIGINT,
    smart_199_raw    BIGINT,
    CONSTRAINT pk PRIMARY KEY (salt_bucket, serial_number, event_date)
);
```

Primary key:

```text
(salt_bucket, serial_number, event_date)
```

Secondary indexes:

```sql
CREATE INDEX idx_failure_date ON smart_telemetry (failure, event_date);
CREATE INDEX idx_smart197_date ON smart_telemetry (smart_197_raw, event_date);
CREATE INDEX idx_smart5_date ON smart_telemetry (smart_5_raw, event_date);
```

---

## Main Benchmark Queries

```sql
SELECT COUNT(*) FROM smart_telemetry WHERE smart_197_raw > 0;
SELECT COUNT(*) FROM smart_telemetry WHERE smart_5_raw > 0;
```

Query plans were compared using Phoenix `EXPLAIN`.

Without indexes, Phoenix used full-table scans:

```text
FULL SCAN OVER SMART_TELEMETRY
```

With indexes, Phoenix used index range scans:

```text
RANGE SCAN OVER IDX_SMART197_DATE
RANGE SCAN OVER IDX_SMART5_DATE
```

---

## Experiment Scales

| Stage   | Dataset Size | Purpose                     |
| ------- | -----------: | --------------------------- |
| Stage 1 |     10K rows | Sanity check                |
| Stage 2 |    100K rows | Baseline benchmark          |
| Stage 3 |    500K rows | Medium load                 |
| Stage 4 |      1M rows | Strong benchmark point      |
| Stage 5 |      2M rows | Larger benchmark            |
| Stage 6 |      5M rows | Main final evaluation scale |
| Stage 7 |     10M rows | Stress test                 |

---

## Key Results

At 5M rows:

| Query               | No Index Avg | With Index Avg | Speedup |
| ------------------- | -----------: | -------------: | ------: |
| `smart_197_raw > 0` |    ~21.046 s |       ~0.134 s | ~157.1x |
| `smart_5_raw > 0`   |    ~18.258 s |       ~0.323 s |  ~56.5x |

The 10M row stress test showed that full scans became expensive and caused scanner lease-related stress, while indexed queries still completed in sub-second time.

---

## Spark Profiling Summary

Spark local mode was used to profile the 5M cleaned dataset.

Important findings:

* Total rows: 5,000,000
* Failure rows: 211, approximately 0.00422%
* `smart_197_raw > 0`: 56,681 rows, approximately 1.13362%
* `smart_5_raw > 0`: 174,603 rows, approximately 3.49206%
* `smart_197_raw > 0 OR smart_5_raw > 0`: 208,765 rows, approximately 4.1753%

The profiling showed that SMART risk indicators were sparse. This helped justify secondary indexes on selected SMART attributes.

---

## Monitoring

Monitoring tools used:

* Prometheus
* Grafana
* node exporter
* JMX exporter

Prometheus targets:

| Target                 | Port |
| ---------------------- | ---: |
| Prometheus             | 9090 |
| node exporter          | 9100 |
| HBase master JMX       | 9404 |
| HBase regionserver JMX | 9405 |

Monitoring was used to observe CPU usage, memory usage, disk I/O, JVM heap, garbage collection, and HBase process metrics during benchmark runs.

---

## How to Reproduce

### 1. Start HBase

```bash
start-hbase.sh
jps
```

Expected HBase processes:

```text
HMaster
HRegionServer
HQuorumPeer
```

### 2. Open Phoenix SQL shell

```bash
~/phoenix/bin/sqlline.py localhost
```

### 3. Create Phoenix table and indexes

Run the schema and index SQL files from:

```text
scripts/benchmark/
```

### 4. Clean Backblaze data

```bash
python3 scripts/cleaning/clean_backblaze_q1_2024.py
```

### 5. Load cleaned CSV into Phoenix

```bash
~/phoenix/bin/psql.py \
  -t SMART_TELEMETRY \
  -h SALT_BUCKET,SERIAL_NUMBER,EVENT_DATE,MODEL,CAPACITY_BYTES,DATACENTER,CLUSTER_ID,FAILURE,SMART_5_RAW,SMART_9_RAW,SMART_187_RAW,SMART_188_RAW,SMART_197_RAW,SMART_198_RAW,SMART_199_RAW \
  localhost clean/smart_telemetry_500k.csv
```

### 6. Run benchmark queries

```bash
~/phoenix/bin/sqlline.py localhost scripts/benchmark/benchmark_queries.sql
```

### 7. Run Spark profiling

```bash
spark-submit scripts/spark/spark_profile.py clean/smart_telemetry_5M.csv results/spark_profile_5M
```

---

## Notes and Limitations

* Experiments were run on a single-node Linux server using pseudo-distributed HBase.
* This repository does not represent a production-grade multi-node deployment.
* CRUD benchmark timings include Phoenix CLI/JVM startup and connection overhead because operations invoked Phoenix separately.
* Concurrency testing was not included in the final evaluation due to time and scope constraints.
* Large raw datasets may be excluded from GitHub if they exceed repository/file size limits.

---

## Authors

Theme 10

* Parth Parashtekar
* Sarthak Sonpatki
* Manas Joshi
* Naman Sharma
* Yash Chaudhari
