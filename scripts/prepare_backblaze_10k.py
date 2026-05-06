import csv
import hashlib
import logging
from pathlib import Path
from datetime import datetime

INPUT = Path("/home/parth/bigdata_project/raw/data_Q1_2024/2024-01-01.csv")
OUTPUT = Path("/home/parth/bigdata_project/clean/smart_telemetry_10k.csv")
LOG_FILE = Path("/home/parth/bigdata_project/logs/2024q1_cleaning.log")

LIMIT = 10000

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def salt_bucket(serial: str, buckets: int = 16) -> int:
    h = hashlib.md5(serial.encode("utf-8")).hexdigest()
    return int(h[:2], 16) % buckets

def safe_int(value, default=0):
    try:
        if value is None:
            return default
        s = str(value).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def main():
    start_time = datetime.now()
    logging.info("=== Cleaning Started ===")
    logging.info(f"Input file: {INPUT}")
    logging.info(f"Output file: {OUTPUT}")
    logging.info(f"Row limit: {LIMIT}")

    processed = 0
    skipped = 0

    try:
        with INPUT.open("r", newline="", encoding="utf-8") as fin, \
             OUTPUT.open("w", newline="", encoding="utf-8") as fout:

            reader = csv.DictReader(fin)
            writer = csv.writer(fout)

            writer.writerow([
                "salt_bucket",
                "serial_number",
                "event_date",
                "model",
                "capacity_bytes",
                "datacenter",
                "cluster_id",
                "failure",
                "smart_5_raw",
                "smart_9_raw",
                "smart_187_raw",
                "smart_188_raw",
                "smart_197_raw",
                "smart_198_raw",
                "smart_199_raw",
            ])

            for row in reader:
                serial = (row.get("serial_number") or "").strip()
                event_date = (row.get("date") or "").strip()

                if not serial or not event_date:
                    skipped += 1
                    continue

                try:
                    writer.writerow([
                        salt_bucket(serial),
                        serial,
                        event_date,
                        (row.get("model") or "").strip(),
                        safe_int(row.get("capacity_bytes"), 0),
                        (row.get("datacenter") or "").strip(),
                        (row.get("cluster_id") or "").strip(),
                        safe_int(row.get("failure"), 0),
                        safe_int(row.get("smart_5_raw"), 0),
                        safe_int(row.get("smart_9_raw"), 0),
                        safe_int(row.get("smart_187_raw"), 0),
                        safe_int(row.get("smart_188_raw"), 0),
                        safe_int(row.get("smart_197_raw"), 0),
                        safe_int(row.get("smart_198_raw"), 0),
                        safe_int(row.get("smart_199_raw"), 0),
                    ])

                    processed += 1

                    if processed % 1000 == 0:
                        logging.info(f"Processed {processed} rows...")

                    if processed >= LIMIT:
                        break

                except Exception as e:
                    skipped += 1
                    logging.warning(f"Row processing error: {e}")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logging.info("=== Cleaning Completed ===")
        logging.info(f"Processed rows: {processed}")
        logging.info(f"Skipped rows: {skipped}")
        logging.info(f"Duration: {duration:.2f} seconds")

        print(f"Wrote {processed} rows to {OUTPUT}")
        print(f"Log file: {LOG_FILE}")

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        print("Script failed. Check log file.")

if __name__ == "__main__":
    main()
