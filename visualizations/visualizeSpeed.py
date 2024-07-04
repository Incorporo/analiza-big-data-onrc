import json
import os
from datetime import datetime
import matplotlib.pyplot as plt
from collections import defaultdict
import pytz
import numpy as np


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
            date = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            romanian_tz = pytz.timezone('Europe/Bucharest')
            return date.astimezone(romanian_tz)
        except ValueError:
            return None
    return None


def process_county_data(data):
    processing_time = defaultdict(lambda: defaultdict(int))
    decision_types = {'Admis', 'Respins', 'Amânat'}

    for item in data:
        resolution_date = parse_date(item.get('resolutionDate'))
        application_date = parse_date(item.get('applicationDate'))
        resolution = item.get('resolution')

        if application_date and resolution_date and resolution in decision_types:
            processing_days = (resolution_date.date() - application_date.date()).days
            processing_days = min(processing_days, 31)  # Group all 31+ days together
            processing_time[processing_days][resolution] += 1

    return processing_time


def visualize_county_data(county, processing_time):
    plt.figure(figsize=(15, 8))
    days = range(32)  # 0 to 31+ days
    decision_types = ['Admis', 'Respins', 'Amânat']
    colors = {'Admis': 'green', 'Respins': 'red', 'Amânat': 'yellow'}

    bottom = np.zeros(32)
    for decision in decision_types:
        frequencies = [processing_time[day][decision] for day in days]
        plt.bar(days, frequencies, bottom=bottom, label=decision, color=colors[decision])
        bottom += frequencies

    plt.xlabel('Processing Time (Days)')
    plt.ylabel('Frequency')
    plt.title(f'Decision Processing Speed for {county}')
    plt.xticks(days, [str(day) if day < 31 else '31+' for day in days])
    plt.legend(title='Decision Type')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    os.makedirs('img/decisions-speed', exist_ok=True)
    plt.savefig(f"img/decisions-speed/{county}_processing_speed.png", dpi=300, bbox_inches='tight')
    plt.close()


def process_all_counties():
    counties_dir = "../results/counties"
    for file_name in os.listdir(counties_dir):
        if file_name.endswith(".json"):
            county = file_name[:-5]  # Remove .json extension
            file_path = os.path.join(counties_dir, file_name)
            data = load_json_file(file_path)
            if data:
                processing_time = process_county_data(data)
                visualize_county_data(county, processing_time)
                print(f"Processed and visualized decision speed data for {county}")


if __name__ == "__main__":
    process_all_counties()
    print("All counties processed. Check the 'img/decisions-speed/' directory for output PNG files.")