import re
import csv
import logging
import boto3
import tempfile
import os

from models import (
    BcRecord, BhRecord, CorpBondRecord, EtfRecord, GlRecord, HlRecord,
    McapRecord, PdRecord, PrRecord, SmeRecord, TtRecord
)
from db_adapter import get_connection_string, SQLAlchemyAdapter

# Initialize S3 client and bucket name
s3 = boto3.client("s3")
BUCKET_NAME = "bhavcopypackage"

# -----------------------
# File Type & Model Mapping
# -----------------------
FILE_TYPE_CONFIG = [
    {"prefix": "Bc",       "parser": "csv", "delimiter": ",", "model": "BcRecord"},
    {"prefix": "bh",       "parser": "csv", "delimiter": ",", "model": "BhRecord",
     "column_map": {"high/low": "high_low"}},
    {"prefix": "corpbond", "parser": "csv", "delimiter": ",", "model": "CorpBondRecord"},
    {"prefix": "etf",      "parser": "csv", "delimiter": ",", "model": "EtfRecord",
     "column_map": {
         "previous_close_price": "prev_close_price",
         "52_week_high": "week_52_high",
         "52_week_low": "week_52_low"
     }},
    {"prefix": "Gl",       "parser": "csv", "delimiter": ",", "model": "GlRecord",
     "column_map": {
         "gain_loss": "gain_or_loss",
         "close_pric": "close_price",
         "prev_cl_pr": "prev_close_price",
         "percent_cg": "percent_change"
     }},
    {"prefix": "HL",       "parser": "csv", "delimiter": ",", "model": "HlRecord"},
    {"prefix": "MCAP",     "parser": "csv", "delimiter": ",", "model": "McapRecord",
     "column_map": {
         "face_value(rs.)": "face_value",
         "close_price_paid_up_value(rs.)": "close_price",
         "market_cap(rs.)": "market_cap"
     }},
    {"prefix": "Pd",       "parser": "csv", "delimiter": ",", "model": "PdRecord"},
    {"prefix": "Pr",       "parser": "csv", "delimiter": ",", "model": "PrRecord"},
    {"prefix": "sme",      "parser": "csv", "delimiter": ",", "model": "SmeRecord"},
    {"prefix": "Tt",       "parser": "csv", "delimiter": ",", "model": "TtRecord"}
]

MODEL_MAPPING = {
    "BcRecord": BcRecord, "BhRecord": BhRecord, "CorpBondRecord": CorpBondRecord,
    "EtfRecord": EtfRecord, "GlRecord": GlRecord, "HlRecord": HlRecord,
    "McapRecord": McapRecord, "PdRecord": PdRecord, "PrRecord": PrRecord,
    "SmeRecord": SmeRecord, "TtRecord": TtRecord
}

# -----------------------
# CSV Parser
# -----------------------
class CsvParser:
    def parse(self, file_path, file_def):
        rows = []
        delimiter = file_def.get("delimiter", ",")
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                rows.append(row)
        return rows

def get_parser(parser_type):
    if parser_type == "csv":
        return CsvParser()
    else:
        raise ValueError(f"Unsupported parser: {parser_type}")

# -----------------------
# Row Key Cleaning
# -----------------------
def process_row_keys(row, file_def):
    cleaned = {}
    column_map = file_def.get("column_map", {})
    numeric_fields = {"close_price", "prev_close_price", "percent_change", "face_value", "market_cap"}

    for raw_key, raw_val in row.items():
        if raw_key is None:
            continue
        key = str(raw_key).strip().lower().replace(" ", "_").replace("/", "_")
        if key in column_map:
            key = column_map[key]

        if isinstance(raw_val, str):
            val = raw_val.strip()
            if val == "":
                cleaned[key] = None
            elif key in numeric_fields:
                try:
                    cleaned[key] = float(val)
                except ValueError:
                    cleaned[key] = None
            else:
                cleaned[key] = val
        else:
            cleaned[key] = raw_val

    return cleaned

# -----------------------
# File Matching
# -----------------------
def match_file(file_name, file_types):
    for file_def in file_types:
        prefix = file_def["prefix"]
        pattern = rf"^{re.escape(prefix)}\d{{6,8}}"
        if re.match(pattern, file_name, re.IGNORECASE):
            return file_def
    return None

# -----------------------
# Main Processing Function
# -----------------------
def process_files(s3_prefix, db_config_path="db_config.json"):
    """
    Streams CSV files from S3, parses them, and bulk-inserts into the database.
    """
    # Initialize DB adapter
    conn_str = get_connection_string(db_config_path)
    db = SQLAlchemyAdapter(conn_str)

    # List all objects under the given S3 prefix
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_prefix)
    contents = resp.get("Contents", [])
    if not contents:
        logging.info(f"No files found under prefix: {s3_prefix}")
        db.close()
        return

    for obj in contents:
        key = obj["Key"]
        file_name = os.path.basename(key)

        # Only process CSV files
        if not file_name.lower().endswith(".csv"):
            continue

        file_def = match_file(file_name, FILE_TYPE_CONFIG)
        if not file_def:
            logging.info(f"Skipping {file_name}: no matching config")
            continue

        parser = get_parser(file_def["parser"])
        logging.info(f"Downloading and parsing: {file_name}")

        try:
            # Download S3 object to a temporary file
            with tempfile.NamedTemporaryFile("w+b", delete=False) as tmp:
                s3.download_fileobj(BUCKET_NAME, key, tmp)
                tmp.flush()
                tmp.seek(0)
                raw_rows = parser.parse(tmp.name, file_def)

            # Build ORM instances
            model_cls = MODEL_MAPPING[file_def["model"]]
            instances = []
            for row in raw_rows:
                try:
                    cleaned = process_row_keys(row, file_def)
                    instances.append(model_cls(**cleaned))
                except Exception as e:
                    logging.error(f"Error processing row in {file_name}: {e} | {row}")

            # Bulk-insert if we have data
            if instances:
                db.bulk_insert(model_cls, instances)
                logging.info(f"Inserted {len(instances)} records from {file_name}")

        except Exception as e:
            logging.error(f"Failed to process {file_name}: {e}")

    # Clean up DB resources
    db.close()
