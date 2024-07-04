import json
import os
from datetime import datetime
import matplotlib.pyplot as plt
from collections import defaultdict
import pytz


def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Invalid JSON in file: {file_path}")
        return None


def parse_date(date_string):
    if date_string:
        try:
            # Parse the date and convert to Romanian time
            date = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            romanian_tz = pytz.timezone('Europe/Bucharest')
            return date.astimezone(romanian_tz)
        except ValueError:
            return None
    return None


def process_county_data(county, data):
    hour_frequency = defaultdict(lambda: defaultdict(int))
    resolution_frequency = defaultdict(lambda: defaultdict(int))
    source_codes = set()
    resolution_types = set()

    for item in data:
        resolution_date = parse_date(item.get('resolutionDate'))
        source_code = item.get('sourceCode', 'Unknown')
        resolution = item.get('resolution')

        if resolution_date:
            hour_frequency[resolution_date.hour][source_code] += 1
            source_codes.add(source_code)

            if resolution:
                resolution_frequency[resolution_date.hour][resolution] += 1
                resolution_types.add(resolution)

    return hour_frequency, source_codes, resolution_frequency, resolution_types


def visualize_county_data(county, hour_frequency, source_codes, resolution_frequency, resolution_types):
    hours = range(24)
    source_codes = sorted(source_codes)
    colors = plt.cm.get_cmap('Set3')(np.linspace(0, 1, len(source_codes)))

    # Source Code Visualization
    plt.figure(figsize=(15, 8))
    bottom = np.zeros(24)

    for i, source_code in enumerate(source_codes):
        frequencies = [hour_frequency[hour][source_code] for hour in hours]
        plt.bar(hours, frequencies, bottom=bottom, label=source_code, color=colors[i])
        bottom += frequencies

    plt.xlabel('Hour of Day (Romanian Time)')
    plt.ylabel('Frequency')
    plt.title(f'Resolution Frequency by Hour and Source Code for {county}')
    plt.xticks(hours)
    plt.legend(title='Source Code', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    os.makedirs('img/programme-source', exist_ok=True)
    plt.savefig(f"img/programme-source/{county}_resolution_frequency.png", dpi=300, bbox_inches='tight')
    plt.close()

    # Resolution Type Visualization
    plt.figure(figsize=(15, 8))
    bottom = np.zeros(24)
    resolution_types = sorted(resolution_types)
    colors = plt.cm.get_cmap('Set2')(np.linspace(0, 1, len(resolution_types)))

    for i, resolution_type in enumerate(resolution_types):
        frequencies = [resolution_frequency[hour][resolution_type] for hour in hours]
        plt.bar(hours, frequencies, bottom=bottom, label=resolution_type, color=colors[i])
        bottom += frequencies

    plt.xlabel('Hour of Day (Romanian Time)')
    plt.ylabel('Frequency')
    plt.title(f'Resolution Type Frequency by Hour for {county}')
    plt.xticks(hours)
    plt.legend(title='Resolution Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    os.makedirs('img/programme-decisions', exist_ok=True)
    plt.savefig(f"img/programme-decisions/{county}_resolution_type_frequency.png", dpi=300, bbox_inches='tight')
    plt.close()


def process_all_counties():
    counties_dir = "../results/counties"
    for file_name in os.listdir(counties_dir):
        if file_name.endswith(".json"):
            county = file_name[:-5]  # Remove .json extension
            file_path = os.path.join(counties_dir, file_name)
            data = load_json_file(file_path)
            if data:
                hour_frequency, source_codes, resolution_frequency, resolution_types = process_county_data(county, data)
                visualize_county_data(county, hour_frequency, source_codes, resolution_frequency, resolution_types)
                print(f"Processed and visualized data for {county}")


if __name__ == "__main__":
    import numpy as np

    process_all_counties()
    print("All counties processed. Check the 'img/programme-decisions/' directory for output PNG files.")