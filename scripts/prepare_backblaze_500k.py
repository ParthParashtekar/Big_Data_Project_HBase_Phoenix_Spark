import csv
import hashlib
import logging
from pathlib import Path
from datetime import datetime

INPUT_DIR = Path("/home/parth/bigdata_project/raw/data_Q1_2024")
OUTPUT = Path("/home/parth/bigdata_project/clean/smart_telemetry_500k.csv")
LOG_FILE = Path("/home/parth/bigdata_project/logs/2024q1_cleaning_500k.log")

LIMIT = 500000

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

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
    processed = 0
    skipped = 0
    files_processed = 0

    logging.info("=== 500K Cleaning Started ===")
    logging.info(f"Input directory: {INPUT_DIR}")
    logging.info(f"Output file: {OUTPUT}")
    logging.info(f"Row limit: {LIMIT}")

    csv_files = sorted(INPUT_DIR.glob("*.csv"))

    if not csv_files:
        logging.error(f"No CSV files found in {INPUT_DIR}")
        print(f"No CSV files found in {INPUT_DIR}")
        return

    try:
        with OUTPUT.open("w", newline="", encoding="utf-8") as fout:
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

            for csv_file in csv_files:
                files_processed += 1
                logging.info(f"Processing file {files_processed}: {csv_file}")

                with csv_file.open("r", newline="", encoding="utf-8") as fin:
                    reader = csv.DictReader(fin)

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

                            if processed % 10000 == 0:
                                logging.info(f"Processed {processed} rows...")

                            if processed >= LIMIT:
                                break

                        except Exception as e:
                            skipped += 1
                            logging.warning(f"Row processing error in {csv_file}: {e}")

                if processed >= LIMIT:
                    break

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logging.info("=== 500K Cleaning Completed ===")
        logging.info(f"Files processed: {files_processed}")
        logging.info(f"Processed rows: {processed}")
        logging.info(f"Skipped rows: {skipped}")
        logging.info(f"Duration: {duration:.2f} seconds")

        print(f"Wrote {processed} rows to {OUTPUT}")
        print(f"Files processed: {files_processed}")
        print(f"Skipped rows: {skipped}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Log file: {LOG_FILE}")

    except Exception as e:
        logging.exception(f"Fatal error during 500K cleaning: {e}")
        print("Script failed. Check log file.")

if __name__ == "__main__":
    main()
