import json
import os
import matplotlib.pyplot as plt
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


def process_county_data(data):
    total_cases = 0
    delayed_cases = 0
    admitted_cases = 0
    denied_cases = 0

    for item in data:
        resolution = item.get('resolution')
        if resolution == 'Admis':
            admitted_cases += 1
            total_cases += 1
        elif resolution == 'Respins':
            denied_cases += 1
            total_cases += 1
        elif resolution == 'Amânat':
            delayed_cases += 1

    return {
        'Delayed': delayed_cases,
        'Admitted': admitted_cases,
        'Denied': denied_cases,
        'Total Cases': total_cases
    }


def visualize_county_data(county, data):
    fig, ax = plt.subplots(figsize=(12, 8))

    categories = ['Admitted', 'Denied', 'Delayed']
    values = [data['Admitted'], data['Denied'], data['Delayed']]

    total = sum(values)
    percentages = [v / total * 100 for v in values]

    colors = ['#32CD32', '#DC143C', '#FFA500']  # Green, Red, Orange

    ax.bar(categories, values, color=colors)

    # Add value labels on top of each bar
    for i, v in enumerate(values):
        ax.text(i, v, f'{v}\n({percentages[i]:.1f}%)', ha='center', va='bottom')

    ax.set_ylabel('Number of Cases')
    ax.set_title(f'Case Outcomes for {county}\nTotal Cases: {data["Total Cases"]}', fontsize=16)

    # Add a text box with additional information
    delayed_percentage = (data['Delayed'] / total) * 100
    info_text = f'Delayed: {data["Delayed"]} ({delayed_percentage:.1f}%)\n'
    info_text += f'Admitted: {data["Admitted"]} ({(data["Admitted"] / total * 100):.1f}%)\n'
    info_text += f'Denied: {data["Denied"]} ({(data["Denied"] / total * 100):.1f}%)'

    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    ax.text(0.05, 0.95, info_text, transform=ax.transAxes, fontsize=12,
            verticalalignment='top', bbox=props)

    plt.tight_layout()

    os.makedirs('img/case-percentages', exist_ok=True)
    plt.savefig(f"img/case-percentages/{county}_case_outcomes.png", dpi=300, bbox_inches='tight')
    plt.close()


def process_all_counties():
    counties_dir = "../results/counties"
    for file_name in os.listdir(counties_dir):
        if file_name.endswith(".json"):
            county = file_name[:-5]  # Remove .json extension
            file_path = os.path.join(counties_dir, file_name)
            data = load_json_file(file_path)
            if data:
                outcome_data = process_county_data(data)
                visualize_county_data(county, outcome_data)
                print(f"Processed and visualized outcome data for {county}")


if __name__ == "__main__":
    process_all_counties()
    print("All counties processed. Check the 'img/case-percentages/' directory for output PNG files.")