from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, sum as spark_sum,
    avg, min as spark_min, max as spark_max,
    expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, DateType
)

INPUT = "/home/parth/bigdata_project/clean/smart_telemetry_5M.csv"
OUTPUT_DIR = Path("/home/parth/bigdata_project/results/spark_profile_5M")
LOG_FILE = Path("/home/parth/bigdata_project/logs/spark_profile_5M.log")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

schema = StructType([
    StructField("salt_bucket", IntegerType(), True),
    StructField("serial_number", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("model", StringType(), True),
    StructField("capacity_bytes", LongType(), True),
    StructField("datacenter", StringType(), True),
    StructField("cluster_id", StringType(), True),
    StructField("failure", IntegerType(), True),
    StructField("smart_5_raw", LongType(), True),
    StructField("smart_9_raw", LongType(), True),
    StructField("smart_187_raw", LongType(), True),
    StructField("smart_188_raw", LongType(), True),
    StructField("smart_197_raw", LongType(), True),
    StructField("smart_198_raw", LongType(), True),
    StructField("smart_199_raw", LongType(), True),
])

def write_single_csv(df, path_name):
    output_path = str(OUTPUT_DIR / path_name)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

def main():
    spark = (
        SparkSession.builder
        .appName("Backblaze SMART Telemetry Profiling")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    with LOG_FILE.open("w") as log:
        log.write("===============================\n")
        log.write("SPARK DATA PROFILING - 5M\n")
        log.write("===============================\n")
        log.write(f"Input: {INPUT}\n")
        log.write(f"Output: {OUTPUT_DIR}\n\n")

        df = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv(INPUT)
        )

        df.cache()

        total_rows = df.count()
        log.write(f"Total rows: {total_rows}\n\n")

        # summary
        summary = df.select(
            count("*").alias("total_rows"),
            spark_sum(when(col("failure") == 1, 1).otherwise(0)).alias("failure_count"),
            spark_sum(when(col("smart_197_raw") > 0, 1).otherwise(0)).alias("smart_197_positive"),
            spark_sum(when(col("smart_5_raw") > 0, 1).otherwise(0)).alias("smart_5_positive"),
            spark_sum(
                when((col("smart_197_raw") > 0) | (col("smart_5_raw") > 0), 1).otherwise(0)
            ).alias("risk_rows"),
            avg("smart_197_raw").alias("avg_smart_197_raw"),
            spark_max("smart_197_raw").alias("max_smart_197_raw"),
            avg("smart_5_raw").alias("avg_smart_5_raw"),
            spark_max("smart_5_raw").alias("max_smart_5_raw"),
            avg("smart_9_raw").alias("avg_smart_9_raw"),
            spark_max("smart_9_raw").alias("max_smart_9_raw"),
        )
        write_single_csv(summary, "summary")

        # failure dist
        failure_distribution = (
            df.groupBy("failure")
            .count()
            .orderBy("failure")
            .withColumn("percentage", (col("count") / total_rows) * 100)
        )
        write_single_csv(failure_distribution, "failure_distribution")

        # risk flags
        risk_distribution = spark.createDataFrame(
            [
                ("smart_197_raw > 0", df.filter(col("smart_197_raw") > 0).count()),
                ("smart_5_raw > 0", df.filter(col("smart_5_raw") > 0).count()),
                (
                    "smart_197_raw > 0 OR smart_5_raw > 0",
                    df.filter((col("smart_197_raw") > 0) | (col("smart_5_raw") > 0)).count(),
                ),
            ],
            ["condition", "count"]
        ).withColumn("percentage", (col("count") / total_rows) * 100)
        write_single_csv(risk_distribution, "risk_distribution")

        # top drive models
        top_models = (
            df.groupBy("model")
            .count()
            .orderBy(col("count").desc())
            .limit(20)
        )
        write_single_csv(top_models, "top_models")

        # records/date
        date_distribution = (
            df.groupBy("event_date")
            .count()
            .orderBy("event_date")
        )
        write_single_csv(date_distribution, "date_distribution")

        # missing counts
        null_counts = df.select([
            spark_sum(
                when(col(c).isNull() | (col(c).cast("string") == ""), 1).otherwise(0)
            ).alias(c)
            for c in df.columns
        ])
        write_single_csv(null_counts, "null_counts")

        # SMART attribute summary
        smart_stats = df.select(
            avg("smart_5_raw").alias("avg_smart_5_raw"),
            spark_min("smart_5_raw").alias("min_smart_5_raw"),
            spark_max("smart_5_raw").alias("max_smart_5_raw"),
            avg("smart_197_raw").alias("avg_smart_197_raw"),
            spark_min("smart_197_raw").alias("min_smart_197_raw"),
            spark_max("smart_197_raw").alias("max_smart_197_raw"),
            avg("smart_198_raw").alias("avg_smart_198_raw"),
            spark_min("smart_198_raw").alias("min_smart_198_raw"),
            spark_max("smart_198_raw").alias("max_smart_198_raw"),
            avg("smart_199_raw").alias("avg_smart_199_raw"),
            spark_min("smart_199_raw").alias("min_smart_199_raw"),
            spark_max("smart_199_raw").alias("max_smart_199_raw"),
        )
        write_single_csv(smart_stats, "smart_stats")

        log.write("Generated outputs:\n")
        log.write("- summary\n")
        log.write("- failure_distribution\n")
        log.write("- risk_distribution\n")
        log.write("- top_models\n")
        log.write("- date_distribution\n")
        log.write("- null_counts\n")
        log.write("- smart_stats\n\n")
        log.write("Spark profiling completed successfully.\n")

        df.unpersist()

    spark.stop()
    print(f"Spark profiling complete. Results written to {OUTPUT_DIR}")
    print(f"Log file: {LOG_FILE}")

if __name__ == "__main__":
    main()
