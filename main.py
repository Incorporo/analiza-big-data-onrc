import requests
import json
import concurrent.futures
from counties import counties
import time
import random
import os
import traceback
from datetime import datetime
import sys
import shutil
import math

# Variables to customize
OUTPUT_FOLDER = "results"
MAX_APPLICATION_NUMBER = 380001
SAVE_FREQUENCY = 250  # Save after every 10 successful requests
MAX_RETRIES = 2
INITIAL_RETRY_DELAY = 0.2
MAX_EMPTY_COUNT = 1500
STATUS_UPDATE_INTERVAL = 1  # seconds
THREAD_MULTIPLIER = 1.5


def get_authorization_key():
    return input("Please enter the Bearer token: ")


def log_error(error_data):
    with open('errors.txt', 'a') as f:
        f.write(json.dumps(error_data, indent=2) + "\n\n")

    # Also write a text version for easier debugging
    with open('errors_debug.txt', 'a') as f:
        f.write(f"Timestamp: {error_data['timestamp']}\n")
        f.write(f"URL: {error_data['url']}\n")
        f.write(f"Payload: {json.dumps(error_data['payload'], indent=2)}\n")
        f.write(f"Error Type: {error_data['error_type']}\n")
        f.write(f"Error Message: {error_data['error_message']}\n")
        f.write(f"Attempt: {error_data['attempt']}\n")
        if 'response_content' in error_data:
            f.write(f"Response Content: {error_data['response_content']}\n")

        f.write("\n" + "-" * 50 + "\n\n")


def make_request(url, payload, headers, retry_count=0):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            response_text = response.text

            # Try reading as JSON, output that we failed if it fails (in log_error)
            try:
                response_json = response.json()
            except json.JSONDecodeError:
                response_json = None
                log_error({
                    "timestamp": datetime.now().isoformat(),
                    "url": url,
                    "payload": payload,
                    "error_type": "JSONDecodeError",
                    "error_message": "Failed to decode JSON response",
                    "response_content": response_text,
                    "attempt": attempt + 1
                })

            return response_json

        except requests.exceptions.RequestException as e:
            error_data = {
                "timestamp": datetime.now().isoformat(),
                "url": url,
                "payload": payload,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "attempt": attempt + 1
            }
            log_error(error_data)
            if attempt == MAX_RETRIES - 1:
                return None
            sleep_time = INITIAL_RETRY_DELAY * (2 ** attempt) + random.uniform(0, 1)
            time.sleep(sleep_time)
    return None


def make_first_request(county, application_number, headers):
    url = "https://api.berc.onrc.ro/client/api/publicitySituations"
    county_data = {k: v for k, v in county.items() if k != 'mnemonic'}
    payload = {
        "county": county_data,
        "fiscalCode": "",
        "applicationNumber": str(application_number),
        "applicationYear": "2024",
        "name": "",
        "listType": "notAll"
    }
    return make_request(url, payload, headers)


def make_second_request(publicity_id, application_number, county, headers):
    url = "https://api.berc.onrc.ro/backoffice/api/article/filter-article?all=notAll&page=0&pageSize=10"
    county_data = {k: v for k, v in county.items() if k != 'mnemonic'}
    payload = {
        "county": county_data,
        "publicityId": publicity_id,
        "applicationNumber": str(application_number),
        "applicationYear": "2024",
        "name": "",
        "listType": "notAll"
    }
    return make_request(url, payload, headers)


def save_results(county, results):
    county_folder = os.path.join(OUTPUT_FOLDER, "counties")
    os.makedirs(county_folder, exist_ok=True)
    filename = f"{county_folder}/{county['name']}.json"

    existing_data = []
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)

    existing_data.extend(results)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)
    print(f"\nSaved {len(results)} new records to {filename}. Total records: {len(existing_data)}")


def get_last_processed_number(county):
    last_processed = 0
    successful_requests = 0

    try:
        filename = f"{OUTPUT_FOLDER}/counties/{county['name']}.json"
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if data:
                    last_entry = data[-1]
                    successful_requests = len(data)
                    if 'publication' in last_entry and 'nr' in last_entry['publication']:
                        last_processed = int(last_entry['publication']['nr'])

    except Exception as e:
        print(f"Error while reading last processed number for {county['name']}: {e}")
        # Shut down all processes
        sys.exit(1)

    return last_processed, successful_requests


def create_progress_bar(percentage, width=100):
    filled_width = int(width * percentage / 100)
    bar = '█' * filled_width + '-' * (width - filled_width)
    return f"[{bar}] {percentage:.2f}%"


def process_county(county, headers):
    all_results = []
    empty_count = 0
    last_processed, successful_requests = get_last_processed_number(county)
    start_number = last_processed + 1
    start_time = datetime.now()
    last_status_update = start_time

    for application_number in range(start_number, MAX_APPLICATION_NUMBER):
        response = make_first_request(county, application_number, headers)

        if response:
            for record in response:
                publicity_id = record["id"]
                response2 = make_second_request(publicity_id, application_number, county, headers)
                if response2:
                    all_results.extend(response2)
                    empty_count = 0
                    successful_requests += 1

                    # Save results after every SAVE_FREQUENCY successful requests
                    if successful_requests % SAVE_FREQUENCY == 0:
                        save_results(county, all_results)
                        all_results = []

            if len(response) == 0:
                empty_count += 1
            else:
                empty_count = 0
        else:
            empty_count += 1

        current_time = datetime.now()
        if (current_time - last_status_update).total_seconds() >= STATUS_UPDATE_INTERVAL:
            elapsed_time = (current_time - start_time).total_seconds() / 60
            rate = (last_processed - start_number) / elapsed_time if elapsed_time > 0 else 0
            percentage_done = application_number / MAX_APPLICATION_NUMBER * 100
            progress_bar = create_progress_bar(percentage_done)
            status_line = f"{county['name']}: {progress_bar} App #{application_number}, Success: {successful_requests}, Rate: {rate:.2f}/min"
            sys.stdout.write('\r' + status_line + ' ' * (shutil.get_terminal_size().columns - len(status_line)))
            sys.stdout.flush()
            last_status_update = current_time

        if empty_count >= MAX_EMPTY_COUNT:
            print(
                f"\nNo data found for {MAX_EMPTY_COUNT} consecutive application numbers. Stopping search for county {county['name']}.")
            break

        time.sleep(random.uniform(0, 0.1))

    # Save any remaining results
    if all_results:
        save_results(county, all_results)

    total_time = (datetime.now() - start_time).total_seconds() / 60
    final_rate = successful_requests / total_time if total_time > 0 else 0
    return f"\nCompleted {county['name']}: Total successful: {successful_requests}, Rate: {final_rate:.2f}/min"


def run():
    try:
        authorization_key = get_authorization_key()
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {authorization_key}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Origin": "https://portal.berc.onrc.ro",
            "Referer": "https://portal.berc.onrc.ro/"
        }

        max_threads = math.ceil(len(counties) * THREAD_MULTIPLIER)
        print(f"Using {max_threads} threads")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_county = {executor.submit(process_county, county, headers): county for county in counties}
            for future in concurrent.futures.as_completed(future_to_county):
                county = future_to_county[future]
                try:
                    result = future.result()
                    print(result)
                except Exception as exc:
                    error_data = {
                        "timestamp": datetime.now().isoformat(),
                        "county": county['name'],
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "traceback": traceback.format_exc()
                    }
                    log_error(error_data)
                    print(f"\nCounty {county['name']} generated an exception. See errors.txt for details.")
    except Exception as e:
        error_data = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc()
        }
        log_error(error_data)
        print("\nUnexpected error in run function. See errors.txt for details.")


if __name__ == "__main__":
    run()