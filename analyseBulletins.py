import queue
import threading
from collections import defaultdict
from concurrent.futures import as_completed, ThreadPoolExecutor
import pymupdf
import re
import os
import pandas as pd
import unicodedata
from fuzzywuzzy import process
from tqdm import tqdm
import time
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Step 1: Extract text from the PDF
ROMANIAN_COUNTIES = [
    "Alba", "Arad", "Argeș", "Bacău", "Bihor", "Bistrița-Năsăud", "Botoșani", "Brăila", "Brașov", "București",
    "Buzău", "Călărași", "Caraș-Severin", "Cluj", "Constanța", "Covasna", "Dâmbovița", "Dolj", "Galați", "Giurgiu",
    "Gorj", "Harghita", "Hunedoara", "Ialomița", "Iași", "Ilfov", "Maramureș", "Mehedinți", "Mureș", "Neamț", "Olt",
    "Prahova", "Sălaj", "Satu Mare", "Sibiu", "Suceava", "Teleorman", "Timiș", "Tulcea", "Vâlcea", "Vaslui", "Vrancea"
]


def extract_text_from_pdf(file_path):
    try:
        text = ""
        pdf_document = pymupdf.open(file_path)
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            text += page.get_text()
        return text
    except Exception as e:
        logging.error(f"Error extracting text from PDF {file_path}: {str(e)}")
        return None


def normalize_text(text):
    """Change diacritics to their base form and convert to lowercase"""
    return ''.join(c for c in unicodedata.normalize('NFD', text)
                   if unicodedata.category(c) != 'Mn')


def get_best_match_county(county):
    """Find the best match for a county name using fuzzy string matching"""
    if not county:
        return None
    normalized_county = normalize_text(county)
    normalized_counties = [normalize_text(c) for c in ROMANIAN_COUNTIES]
    best_match = process.extractOne(normalized_county, normalized_counties)
    if best_match and best_match[1] >= 80:  # 80% similarity threshold
        return ROMANIAN_COUNTIES[normalized_counties.index(best_match[0])]
    return None


# Step 2: Define patterns for extraction with optional "in calitate de" and including county
def extract_information_with_county(text):
    decisions = []
    # Define the pattern for decisions using the updated regex
    decision_pattern = re.compile(r"R O M Â N I A(.*?)Data:\s*(\d{2}\.\d{2}\.\d{4})", re.DOTALL)
    matches = decision_pattern.findall(text)

    for match in matches:
        decision = {}
        content, pronounced_date = match
        # Extract dossier number, decision number, etc.
        dossier_number = re.search(r"DOSAR NR\.\s*(\S+)", content)
        decision_number = re.search(r"ÎNCHEIERE nr\.\s*(\d+)", content)
        firm_name = re.search(r"Firma:\s*(.*?)(?:\n|$)", content)
        address = re.search(r"Sediul:\s*(.*?)(?:\n|$)", content)
        county = re.search(r"de pe lângă Tribunalul ([^\s]+)", content)
        registration_code = re.search(r"Cod unic de înregistrare:\s*(\d+)", content)
        registration_order = re.search(r"Număr de ordine în registrul comerțului:\s*(.*?)(?:\n|$)", content)
        euid = re.search(r"Identificator unic la nivel european \(EUID\):\s*(.*?)(?:\n|$)", content)
        registrator = re.search(r"Registratorul de registrul comerțului:\s*(.*?)(?:\n|$)", content)

        # Extract additional details from the decision text
        requestor_quality = re.search(
            r"formulată de\s+(.*?)\s+(?:în calitate de\s+(.*?)\s+)?privind\s+(.*?)(?=\nExaminând)", content, re.DOTALL)
        disposition_text = re.search(r"D I S P U N E(.*?)Registrator de registrul comerțului", content, re.DOTALL)

        decision['dossier_number'] = dossier_number.group(1) if dossier_number else None
        decision['decision_number'] = decision_number.group(1) if decision_number else None
        decision['pronounced_date'] = pronounced_date
        decision['firm_name'] = firm_name.group(1).strip().replace('\n', ' ') if firm_name else None
        decision['address'] = address.group(1).strip().replace('\n', ' ') if address else None
        decision['county'] = get_best_match_county(county.group(1).strip()) if county else None
        decision['registration_code'] = registration_code.group(1) if registration_code else None
        decision['registration_order'] = registration_order.group(1).strip().replace('\n',
                                                                                     ' ') if registration_order else None
        decision['euid'] = euid.group(1).strip().replace('\n', ' ') if euid else None
        decision['registrator'] = registrator.group(1).strip().replace('\n', ' ') if registrator else None

        if requestor_quality:
            decision['requestor'] = requestor_quality.group(1).strip().replace('\n', ' ')
            decision['quality'] = requestor_quality.group(2).strip().replace('\n', ' ') if requestor_quality.group(
                2) else None
            decision['request_details'] = requestor_quality.group(3).strip().replace('\n', ' ')
        else:
            decision['requestor'] = None
            decision['quality'] = None
            decision['request_details'] = None

        decision['disposition_text'] = disposition_text.group(1).strip().replace('\n',
                                                                                 '\\n') if disposition_text else None

        decisions.append(decision)

    return decisions

# Global queue to hold extracted data
data_queue = queue.Queue()

# Flag to signal the saving thread to stop
stop_flag = threading.Event()

# Accumulated data for batch saving
accumulated_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))


def process_file(file_path, output_directory):
    try:
        filename = os.path.basename(file_path)
        logging.info(f"Processing file: {filename}")

        text = extract_text_from_pdf(file_path)
        logging.debug(f"Extracted text from {filename}")

        decisions = extract_information_with_county(text)
        logging.debug(f"Extracted {len(decisions)} decisions from {filename}")

        if decisions:
            df = pd.DataFrame(decisions)
            if 'county' in df.columns and df['county'].notna().any():
                county_month_data = defaultdict(lambda: defaultdict(list))

                for _, row in df.iterrows():
                    county = row['county']
                    if pd.notna(county):
                        month = pd.to_datetime(row['pronounced_date'], format='%d.%m.%Y').strftime('%B')
                        county_month_data[county][month].append(row.to_dict())

                # Submit the extracted data to the queue
                data_queue.put((county_month_data, output_directory))
                logging.info(f"Data from {filename} added to queue")

        return f"Processed file {filename}"
    except Exception as e:
        logging.error(f"Error processing file {filename}: {str(e)}")
        return f"Error processing file {filename}: {str(e)}"


def save_data_thread():
    global accumulated_data
    last_save_time = time.time()

    while not stop_flag.is_set():
        try:
            county_month_data, output_directory = data_queue.get(timeout=1)
            logging.debug(f"Retrieved data from queue for {output_directory}")

            # Accumulate data
            for county, month_data in county_month_data.items():
                for month, decisions in month_data.items():
                    accumulated_data[output_directory][county][month].extend(decisions)
            logging.debug(
                f"Accumulated data: {sum(len(decisions) for county_data in accumulated_data[output_directory].values() for decisions in county_data.values())} decisions")

            # Check if it's time to save
            current_time = time.time()
            if current_time - last_save_time >= 5:
                save_accumulated_data()
                last_save_time = current_time

            data_queue.task_done()
        except queue.Empty:
            # If queue is empty, check if it's time to save
            current_time = time.time()
            if current_time - last_save_time >= 5:
                save_accumulated_data()
                last_save_time = current_time
        except Exception as e:
            logging.error(f"Error in save_data_thread: {str(e)}")

    # Final save after processing all files
    save_accumulated_data()


def save_accumulated_data():
    global accumulated_data
    logging.info("Saving accumulated data")
    for output_directory, county_data in accumulated_data.items():
        for county, month_data in county_data.items():
            county_dir = os.path.join(output_directory, county)
            os.makedirs(county_dir, exist_ok=True)

            for month, decisions in month_data.items():
                output_file_path = os.path.join(county_dir, f"{month}.csv")

                df = pd.DataFrame(decisions)
                logging.debug(f"Saving {len(df)} decisions for {county} - {month}")

                with threading.Lock():
                    if os.path.exists(output_file_path):
                        # Append to existing file without writing the header
                        df.to_csv(output_file_path, mode='a', header=False, index=False)
                    else:
                        # Create new file with header
                        df.to_csv(output_file_path, index=False)

    # Clear accumulated data after saving
    accumulated_data.clear()
    logging.info("Accumulated data saved and cleared")


def process_all_files_in_directory(input_directory, output_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    file_paths = [os.path.join(input_directory, filename)
                  for filename in os.listdir(input_directory)
                  if filename.endswith(".pdf")]

    total_files = len(file_paths)
    logging.info(f"Found {total_files} PDF files to process")

    # Start the saving thread
    save_thread = threading.Thread(target=save_data_thread)
    save_thread.start()

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_file, file_path, output_directory)
                   for file_path in file_paths]

        with tqdm(total=total_files, desc="Processing files", unit="file") as progress_bar:
            for future in as_completed(futures):
                result = future.result()
                progress_bar.update(1)
                progress_bar.set_postfix_str(result)

    # Signal the saving thread to stop
    logging.info("All files processed, signaling save thread to stop")
    stop_flag.set()

    # Wait for the queue to be empty and the saving thread to finish
    data_queue.join()
    save_thread.join()

    # Perform a final save to ensure all data is persisted
    save_accumulated_data()


def main():
    year = "2024"  # You can modify this to take user input or as a command-line argument
    input_directory = f'bulletins/{year}/'
    output_directory = 'bulletins-analysis/counties-new/'

    # Process files
    process_all_files_in_directory(input_directory, output_directory)

    logging.info(f"Processing complete for year {year}")


if __name__ == "__main__":
    # Set logging level to WARNING to avoid debug messages
    logging.getLogger().setLevel(logging.WARNING)
    main()
