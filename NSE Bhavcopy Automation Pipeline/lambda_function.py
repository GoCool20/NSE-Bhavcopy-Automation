import requests
import zipfile
import io
import boto3
from datetime import datetime, timedelta
import logging
from processor import process_files

s3 = boto3.client('s3')
BUCKET_NAME = 'bhavcopypackage'
S3_KEY = 'bhavcopyzips'


def create_session_directory():
    session_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = f"{S3_KEY}/session_{session_time}"
    zip_path = f"{base_path}/downloaded_zips"
    extract_path = f"{base_path}/extracted_files"

    return {
        'base_prefix': base_path,
        'zip_path': zip_path,
        'extract_path': extract_path,
        'session_id': session_time
    }


def download_today_bhavcopy():
    dirs = create_session_directory()

    NSE_HOLIDAYS = [
        "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
        "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
        "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
        "2025-11-05", "2025-12-25"
    ]

    def get_trading_day(current_date=None):
        if current_date is None:
            current_date = datetime.now().date()
        while current_date.weekday() >= 5 or current_date.strftime("%Y-%m-%d") in NSE_HOLIDAYS:
            current_date -= timedelta(days=1)
        return current_date

    last_day = get_trading_day()
    day = last_day.strftime('%d')
    month = last_day.strftime('%m')
    year_short = last_day.strftime('%y')

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        url = f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{day}{month}{year_short}.zip"

        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            s3.put_object(Bucket=BUCKET_NAME, Key=f"{dirs['zip_path']}/PR{day}{month}{year_short}.zip",
                          Body=response.content)

            zip_key = f"{dirs['zip_path']}/PR{day}{month}{year_short}.zip"
            print(zip_key)
            zip_obj = s3.get_object(Bucket=BUCKET_NAME, Key=zip_key)
            zip_data = zip_obj['Body'].read()

            with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
                for name in z.namelist():
                    print(name)
                    file_bytes = z.read(name)
                    s3.put_object(Bucket=BUCKET_NAME, Key=f"{dirs['extract_path']}/{name}", Body=file_bytes)

        return dirs


    except Exception as e:
        return e


def lambda_handler(event, context):
    dirs = download_today_bhavcopy()
    extract_path = str(dirs["extract_path"])
    process_files(extract_path)