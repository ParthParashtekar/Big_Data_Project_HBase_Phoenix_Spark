import subprocess
import time
import statistics
from pathlib import Path

PHOENIX_BIN = Path("/home/parth/phoenix/bin")
RESULTS_DIR = Path("/home/parth/bigdata_project/results")
LOG_FILE = RESULTS_DIR / "crud_benchmark_results.txt"
TEMP_SQL = RESULTS_DIR / "temp_crud.sql"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def run_sql(sql: str) -> float:
    TEMP_SQL.write_text(sql)
    start = time.time()
    result = subprocess.run(
        [str(PHOENIX_BIN / "psql.py"), "localhost", str(TEMP_SQL)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False
    )
    end = time.time()
    if result.returncode != 0:
        print(f"Warning: SQL may have failed:\n{sql}")
    return end - start

def summarize(name, timings):
    return {
        "operation": name,
        "count": len(timings),
        "avg": statistics.mean(timings),
        "min": min(timings),
        "max": max(timings),
        "total": sum(timings),
    }

def main():
    results = []

    # single inserts
    insert_times = []
    for i in range(100):
        sql = f"""
UPSERT INTO smart_telemetry_crud_test (
    salt_bucket, serial_number, event_date, model,
    capacity_bytes, failure, smart_197_raw, smart_5_raw
)
VALUES (
    0, 'CRUD_TEST_{i}', DATE '2024-04-01', 'TEST_MODEL',
    1000000000, 0, 0, 0
);
"""
        insert_times.append(run_sql(sql))
    results.append(summarize("single_row_insert_100_rows", insert_times))

    # update non idx col: model
    update_non_index_times = []
    for i in range(100):
        sql = f"""
UPSERT INTO smart_telemetry_crud_test (
    salt_bucket, serial_number, event_date, model
)
VALUES (
    0, 'CRUD_TEST_{i}', DATE '2024-04-01', 'UPDATED_MODEL'
);
"""
        update_non_index_times.append(run_sql(sql))
    results.append(summarize("update_non_indexed_column_model_100_rows", update_non_index_times))

    # update idx_col: smart_197_raw
    update_index_times = []
    for i in range(100):
        sql = f"""
UPSERT INTO smart_telemetry_crud_test (
    salt_bucket, serial_number, event_date, smart_197_raw
)
VALUES (
    0, 'CRUD_TEST_{i}', DATE '2024-04-01', 999
);
"""
        update_index_times.append(run_sql(sql))
    results.append(summarize("update_indexed_column_smart197_100_rows", update_index_times))

    # Delet
    delete_times = []
    for i in range(100):
        sql = f"""
DELETE FROM smart_telemetry_crud_test
WHERE salt_bucket = 0
  AND serial_number = 'CRUD_TEST_{i}'
  AND event_date = DATE '2024-04-01';
"""
        delete_times.append(run_sql(sql))
    results.append(summarize("delete_by_primary_key_100_rows", delete_times))

    with LOG_FILE.open("w") as f:
        f.write("===============================\n")
        f.write("CRUD BENCHMARK RESULTS\n")
        f.write("===============================\n\n")
        f.write("Table: smart_telemetry_crud_test\n")
        f.write("Rows per operation: 100\n\n")

        for r in results:
            f.write(f"Operation: {r['operation']}\n")
            f.write(f"Rows: {r['count']}\n")
            f.write(f"Average Time: {r['avg']:.4f} seconds\n")
            f.write(f"Min Time: {r['min']:.4f} seconds\n")
            f.write(f"Max Time: {r['max']:.4f} seconds\n")
            f.write(f"Total Time: {r['total']:.4f} seconds\n")
            f.write("--------------------------------\n")

    print(f"CRUD benchmark complete.")
    print(f"Results written to: {LOG_FILE}")

if __name__ == "__main__":
    main()
