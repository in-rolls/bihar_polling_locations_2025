#!/usr/bin/env python3
"""
Download unique photos from Google Drive links in photo-links CSV files.
Naming format: district-constituency-polling-station-type-filename.ext
"""

import csv
import os
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import random

def extract_file_id(drive_url):
    """Extract Google Drive file ID from URL."""
    if not drive_url or drive_url.strip() == '':
        return None

    # Handle different Google Drive URL formats
    # https://drive.google.com/open?id=FILE_ID
    # https://drive.google.com/file/d/FILE_ID/view

    match = re.search(r'id=([a-zA-Z0-9_-]+)', drive_url)
    if match:
        return match.group(1)

    match = re.search(r'/d/([a-zA-Z0-9_-]+)', drive_url)
    if match:
        return match.group(1)

    return None

def sanitize_filename(name):
    """Sanitize filename to remove invalid characters."""
    # Replace spaces and special chars with underscores
    name = re.sub(r'[^\w\-.]', '_', name)
    # Remove multiple underscores
    name = re.sub(r'_+', '_', name)
    return name

# Global lock for thread-safe printing
print_lock = threading.Lock()

def download_file(file_id, output_path, max_retries=3):
    """Download file from Google Drive using gdown with retry logic."""
    if os.path.exists(output_path):
        return 'skipped'

    # Add random delay to avoid rate limiting (0.5-2 seconds)
    time.sleep(random.uniform(0.5, 2.0))

    for attempt in range(max_retries):
        try:
            # Use gdown to download
            cmd = ['gdown', f'https://drive.google.com/uc?id={file_id}', '-O', output_path, '--quiet']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)

            if result.returncode == 0 and os.path.exists(output_path):
                # Verify file is not empty
                if os.path.getsize(output_path) > 0:
                    with print_lock:
                        print(f"  ✓ {os.path.basename(output_path)}")
                    return 'success'
                else:
                    os.remove(output_path)

            # If failed, check if it's a rate limit error
            error_msg = result.stderr.lower() if result.stderr else ""
            if 'quota' in error_msg or 'limit' in error_msg or 'too many' in error_msg:
                # Exponential backoff for rate limiting
                wait_time = (2 ** attempt) * (1 + random.random())
                with print_lock:
                    print(f"  ⏸ Rate limit hit, waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            elif attempt < max_retries - 1:
                # General retry with shorter delay
                time.sleep(1 + random.random())
                continue
            else:
                with print_lock:
                    print(f"  ✗ Failed after {max_retries} attempts: {os.path.basename(output_path)}")
                return 'error'

        except subprocess.TimeoutExpired:
            if attempt < max_retries - 1:
                with print_lock:
                    print(f"  ⏱ Timeout, retrying... {os.path.basename(output_path)}")
                time.sleep(2)
                continue
            else:
                with print_lock:
                    print(f"  ✗ Timeout: {os.path.basename(output_path)}")
                return 'error'
        except Exception as e:
            with print_lock:
                print(f"  ✗ Error: {os.path.basename(output_path)} - {e}")
            return 'error'

    return 'error'

def download_task(args):
    """Wrapper for parallel download."""
    file_id, output_path = args
    return download_file(file_id, output_path)

def process_csv_file(csv_path, output_dir, max_workers=3):
    """Process a single photo-links CSV file with parallel downloads (reduced to avoid rate limits)."""
    # Extract district name from filename (e.g., "1-Paschim Champaran-photo-links.csv")
    filename = os.path.basename(csv_path)
    district = filename.replace('-photo-links.csv', '')

    print(f"\nProcessing: {filename}")
    print(f"District: {district}")

    # Collect all download tasks
    download_tasks = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Extract information from the row
            ac_name_full = row.get('AC No. & AC Name', '')
            polling_station = row.get('Polling Station No.', '').strip()
            ps_type = row.get('Polling Station Type', '').strip()

            # Extract constituency from AC name (e.g., "4-Bagaha [1-Paschim Champaran]" -> "4-Bagaha")
            constituency_match = re.match(r'([^[]+)', ac_name_full)
            constituency = constituency_match.group(1).strip() if constituency_match else 'Unknown'
            constituency = sanitize_filename(constituency)

            # Sanitize components
            district_safe = sanitize_filename(district)
            polling_station_safe = polling_station.zfill(3)  # Pad with zeros
            ps_type_safe = sanitize_filename(ps_type)

            # Process both photo columns
            photo_columns = [
                ('Photo of Polling Station Building (PSB)', 'PSB'),
                ('Photo of Polling Station Premises with PS Building (PSP)', 'PSP')
            ]

            for col_name, photo_type in photo_columns:
                drive_url = row.get(col_name, '')
                if not drive_url or drive_url.strip() == '':
                    continue

                file_id = extract_file_id(drive_url)
                if not file_id:
                    continue

                # Create structured filename
                # Format: district-constituency-polling-station-type-fileID.jpg
                output_filename = f"{district_safe}-{constituency}-PS{polling_station_safe}-{ps_type_safe}-{photo_type}-{file_id}.jpg"
                output_path = os.path.join(output_dir, output_filename)

                download_tasks.append((file_id, output_path))

    # Execute downloads in parallel
    downloaded_count = 0
    skipped_count = 0
    error_count = 0

    print(f"  Downloading {len(download_tasks)} photos with {max_workers} parallel workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_task, task) for task in download_tasks]

        for future in as_completed(futures):
            result = future.result()
            if result == 'success':
                downloaded_count += 1
            elif result == 'skipped':
                skipped_count += 1
            else:
                error_count += 1

    print(f"\nSummary for {district}:")
    print(f"  Downloaded: {downloaded_count}")
    print(f"  Skipped (existing): {skipped_count}")
    print(f"  Errors: {error_count}")

    return downloaded_count, skipped_count, error_count

def main():
    """Main function to process all photo-links CSV files."""
    # Check if gdown is installed
    try:
        subprocess.run(['gdown', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: gdown is not installed. Installing...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', 'gdown'], check=True)

    # Create output directory for photos
    output_dir = 'photos'
    os.makedirs(output_dir, exist_ok=True)

    # Find all photo-links CSV files
    csv_files = sorted(Path('.').glob('*-photo-links.csv'))

    print(f"Found {len(csv_files)} photo-links CSV files")

    total_downloaded = 0
    total_skipped = 0
    total_errors = 0

    for csv_file in csv_files:
        downloaded, skipped, errors = process_csv_file(str(csv_file), output_dir)
        total_downloaded += downloaded
        total_skipped += skipped
        total_errors += errors

    print(f"\n{'='*60}")
    print(f"TOTAL SUMMARY:")
    print(f"  Total Downloaded: {total_downloaded}")
    print(f"  Total Skipped (existing): {total_skipped}")
    print(f"  Total Errors: {total_errors}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
