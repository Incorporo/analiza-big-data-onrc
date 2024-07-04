import requests
import os
import time
import random
import re
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed
from glob import glob

BASE_URL = "https://api.berc.onrc.ro/backoffice/api/publication"
DMS_URL = "https://dms.berc.onrc.ro"
OUTPUT_DIR = "bulletins"
MAX_GAP_SIZE = 10


def get_headers(auth_token):
    return {
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    }


def get_publication_info(year, number, auth_token):
    url = f"{BASE_URL}/getPublicationByYearAndNumber"
    data = {"number": number, "year": year}
    response = requests.post(url, headers=get_headers(auth_token), json=data)
    if response.status_code == 200 and response.json():
        return response.json()[0]
    return None


def get_download_link(document_id, auth_token):
    url = f"{BASE_URL}/viewPublication"
    data = {"documentId": document_id, "type": "BULETIN"}
    response = requests.post(url, headers=get_headers(auth_token), json=data)
    if response.status_code == 200:
        return response.json().get("downloadLink")
    return None


def extract_csrf_token(html_content):
    match = re.search(r"util\.getCSRF\s*=\s*function\s*\(\)\s*{\s*return\s*'([^']+)';", html_content)
    if match:
        return match.group(1)
    return None


def download_bulletin(year, number, download_link, auth_token, max_retries=5):
    output_dir = os.path.join(OUTPUT_DIR, str(year))
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{number}.pdf")

    session = requests.Session()

    for attempt in range(max_retries):
        try:
            response = session.get(download_link)
            response.raise_for_status()

            csrf_token = extract_csrf_token(response.text)
            if not csrf_token:
                print(f"Failed to extract CSRF token for bulletin {year}/{number}")
                return False

            temp_token = download_link.split('token=')[1].split('&')[0]

            download_url = f"{DMS_URL}/download_file"
            download_data = {
                "X-CSRF-Token": csrf_token,
                "tempToken": temp_token,
                "location": "FILE",
                "nullContentType": "false"
            }
            download_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "Referer": download_link
            }

            file_response = session.post(download_url, data=download_data, headers=download_headers)
            file_response.raise_for_status()

            content_type = file_response.headers.get('Content-Type', '').lower()
            if 'application/pdf' not in content_type:
                print(f"Warning: Content-Type is not PDF for bulletin {year}/{number}. Got: {content_type}")
                return False

            with open(output_path, "wb") as f:
                f.write(file_response.content)
            print(f"Downloaded bulletin {year}/{number}")
            return True

        except RequestException as e:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"Attempt {attempt + 1} failed for bulletin {year}/{number}. Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)

    print(f"Failed to download bulletin {year}/{number} after {max_retries} attempts")
    return False


def get_downloaded_bulletins(year):
    year_dir = os.path.join(OUTPUT_DIR, str(year))
    if not os.path.exists(year_dir):
        return set()
    files = glob(os.path.join(year_dir, "*.pdf"))
    return set(int(os.path.splitext(os.path.basename(f))[0]) for f in files)


def process_bulletin(args):
    year, number, auth_token = args
    publication_info = get_publication_info(year, number, auth_token)
    if publication_info:
        document_id = publication_info.get("versionId")
        if document_id:
            download_link = get_download_link(document_id, auth_token)
            if download_link:
                return number if download_bulletin(year, number, download_link, auth_token) else None
    return None


def process_year(year, auth_token):
    downloaded_bulletins = get_downloaded_bulletins(year)
    start_number = max(downloaded_bulletins) + 1 if downloaded_bulletins else 1
    max_bulletins = 100000

    with ThreadPoolExecutor(max_workers=4) as executor:
        bulletin_args = [(year, number, auth_token) for number in range(start_number, max_bulletins + 1)]
        futures = [executor.submit(process_bulletin, args) for args in bulletin_args]

        consecutive_failures = 0
        max_consecutive_failures = 100
        last_successful = start_number - 1

        for future in as_completed(futures):
            result = future.result()
            if result:
                consecutive_failures = 0
                last_successful = max(last_successful, result)
            else:
                consecutive_failures += 1

            if consecutive_failures >= max_consecutive_failures:
                print(f"Reached {max_consecutive_failures} consecutive failures for year {year}. Moving to next year.")
                break

    # Recovery for small gaps
    all_downloaded = get_downloaded_bulletins(year)
    gaps = []
    for i in range(1, last_successful + 1):
        if i not in all_downloaded:
            gaps.append(i)

    if gaps:
        print(f"Found {len(gaps)} gaps in year {year}. Attempting to recover...")
        for gap_start in range(0, len(gaps), MAX_GAP_SIZE):
            gap_end = min(gap_start + MAX_GAP_SIZE, len(gaps))
            gap_range = gaps[gap_start:gap_end]
            with ThreadPoolExecutor(max_workers=MAX_GAP_SIZE) as executor:
                gap_args = [(year, number, auth_token) for number in gap_range]
                list(executor.map(process_bulletin, gap_args))

    return last_successful > start_number


def main():
    auth_token = input("Please enter your authentication token: ")

    current_year = 2024
    years_without_bulletins = 0
    max_years_without_bulletins = 3

    while years_without_bulletins < max_years_without_bulletins:
        print(f"Processing year: {current_year}")
        bulletins_found = process_year(current_year, auth_token)

        if not bulletins_found:
            years_without_bulletins += 1
            print(f"No bulletins found for year {current_year}. Years without bulletins: {years_without_bulletins}")
        else:
            years_without_bulletins = 0  # Reset counter if we found bulletins

        current_year -= 1

    print(f"No bulletins found for {max_years_without_bulletins} consecutive years. Stopping.")


if __name__ == "__main__":
    main()