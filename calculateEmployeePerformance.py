import pandas as pd
import unicodedata
import os
from glob import glob

def normalize_text(text):
    """Change diacritics to their base form, convert to lowercase, and sort words"""
    if pd.isna(text) or not isinstance(text, str):
        return text
    normalized = ''.join(c for c in unicodedata.normalize('NFD', str(text).lower())
                         if unicodedata.category(c) != 'Mn')
    return ' '.join(sorted(normalized.split()))

# Step 1: Read all CSV files from counties-new/[County]/[Months].csv
base_path = 'bulletins-analysis/counties-new'
output_file_path = 'bulletins-analysis/performance-revision.xlsx'

# Function to read and process a single CSV file
def process_csv(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
        if df.empty:
            print(f"Warning: Empty file: {file_path}")
            return None
        df['original_name'] = df['registrator'].astype(str).str.replace('-', ' ')
        df['registrator'] = df['original_name'].apply(normalize_text)
        df = df[df['registrator'].str.len() <= 45]
        df['pronounced_date'] = pd.to_datetime(df['pronounced_date'], format='%d.%m.%Y', errors='coerce')
        df = df.dropna(subset=['pronounced_date'])
        if df.empty:
            print(f"Warning: No valid data after processing: {file_path}")
            return None
        return df
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None

# Read and concatenate all CSV files
all_files = glob(os.path.join(base_path, '*', '*.csv'))
if not all_files:
    raise ValueError(f"No CSV files found in {base_path}")

print(f"Found {len(all_files)} CSV files.")

df_list = [process_csv(file) for file in all_files]
df_list = [df for df in df_list if df is not None]

if not df_list:
    raise ValueError("No valid data found in any of the CSV files")

df = pd.concat(df_list, ignore_index=True)

# Count total rows and rows with missing dates
total_rows = sum(len(df) for df in df_list)
rows_with_data = len(df)
missing_dates = total_rows - rows_with_data

print(f"Total rows in all files: {total_rows}")
print(f"Rows with valid data: {rows_with_data}")
print(f"Rows dropped due to missing dates: {missing_dates}")

# Step 2: Calculate performance metrics and find most frequent original name
days_worked = df.groupby('registrator')['pronounced_date'].nunique()
dossiers_processed = df.groupby('registrator').size()

# Find most frequent original name for each normalized name
most_frequent_names = df.groupby('registrator')['original_name'].agg(
    lambda x: x.value_counts().index[0]
)

performance_df = pd.DataFrame({
    'most_frequent_name': most_frequent_names,
    'days_worked': days_worked,
    'dossiers_processed': dossiers_processed
}).reset_index()

# Step 3: Calculate performance ratios
performance_df['dossiers_per_day'] = performance_df['dossiers_processed'] / performance_df['days_worked']
performance_df['dossiers_per_hour'] = performance_df['dossiers_per_day'] / 8  # Assuming 8-hour workday

# Sort the DataFrame by dossiers_per_day in descending order
performance_df = performance_df.sort_values('dossiers_per_day', ascending=False)

# Rename columns for clarity
performance_df = performance_df.rename(columns={
    'registrator': 'normalized_name',
    'most_frequent_name': 'registrator'
})

# Reorder columns
column_order = ['registrator', 'normalized_name', 'days_worked', 'dossiers_processed', 'dossiers_per_day', 'dossiers_per_hour']
performance_df = performance_df[column_order]

# Step 4: Save the results as performance.xlsx (excel file) and performance.csv with UTF-8 encoding
performance_df.to_excel(output_file_path, index=False)
performance_df.to_csv(output_file_path.replace('.xlsx', '.csv'), index=False, encoding='utf-8')

print(f"Performance files saved to {output_file_path} and {output_file_path.replace('.xlsx', '.csv')}")
print(f"Total rows processed: {total_rows}")
print(f"Rows with valid dates: {rows_with_data}")
print(f"Number of rows with missing dates dropped: {missing_dates}")