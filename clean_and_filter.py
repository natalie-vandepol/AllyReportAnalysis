import pandas as pd
import os


# Load the dataset
file_path = 'courses.csv'  # Update this with the correct path to your file
data = pd.read_csv(file_path)
columns_to_keep = [
    'Term name',       # Column B
    'Department name', # Column D
    'Course code',     # Column F
    'Course name',      # Column G
    'Course url',       # Column H
    'Total files',     # Column M
    'Overall score', # Column O
    'Files score'     # Column P
]
data = data[columns_to_keep]
relevant_semesters = [
        '2023 Spring', '2023 Summer', '2023 Fall',
        '2024 Spring', '2024 Summer', '2024 Fall',
        '2025 Spring'
    ]

# Step 1: Filter by relevant semesters
def filter_semesters(df):
    return df[df['Term name'].isin(relevant_semesters)]

# Step 2: Filter by relevant departments
def filter_departments(df):
    relevant_departments = [
        "BUSNS DEAN", "HOSP BUS", "FINANCE", "ACCOUNTING",
        "SUPPLY CHN", "MANAGEMENT", "MARKETING"
    ]
    return df[df['Department name'].str.contains('|'.join(relevant_departments), na=False)]

# Step 3: Clean department names
def clean_department_names(df):
    replacements = {
        "BUSNS DEAN": "BUS DN",
        "Michigan State University; BUSNS DEAN": "BUS DN",
        "BUSNS DEAN; Michigan State University": "BUS DN",
        "HOSP BUS": "HOSP",
        "Michigan State University; HOSP BUS": "HOSP",
        "HOSP BUS; Michigan State University": "HOSP",
        "FINANCE": "FIN",
        "FINANCE; Michigan State University": "FIN",
        "Michigan State University; FINANCE": "FIN",
        "MARKETING": "MKT",
        "Michigan State University; MARKETING": "MKT",
        "MARKETING; Michigan State University": "MKT",
        "MANAGEMENT": "MGMT",
        "MANAGEMENT; Michigan State University": "MGMT",
        "Michigan State University; MANAGEMENT": "MGMT",
        "ACCOUNTING": "ACCT",
        "ACCOUNTING; Michigan State University": "ACCT",
        "Michigan State University; ACCOUNTING": "ACCT",
        "SUPPLY CHN": "SCM",
        "SUPPLY CHN; Michigan State University": "SCM",
        "Michigan State University; SUPPLY CHN": "SCM",
        "BUSNS DEAN; MARKETING": "BUS DN; MKT",
        "BUSNS DEAN; MARKETING; Michigan State University": "BUS DN; MKT",
        "BUSNS DEAN; Michigan State University; MARKETING": "BUS DN; MKT",
        "MARKETING; BUSNS DEAN": "BUS DN; MKT",
        "MARKETING; BUSNS DEAN; Michigan State University": "BUS DN; MKT",
        "MARKETING; Michigan State University; BUSNS DEAN": "BUS DN; MKT",
        "Michigan State University; BUSNS DEAN; MARKETING": "BUS DN; MKT",
        "Michigan State University; MARKETING; BUSNS DEAN": "BUS DN; MKT",
        "SUPPLY CHN; MANAGEMENT": "SCM; MGT",
        "MANAGEMENT; SUPPLY CHN": "SCM; MGT",
        "SUPPLY CHN; Michigan State University; MANAGEMENT": "SCM; MGT",
        "SUPPLY CHN; MANAGEMENT; Michigan State University": "SCM; MGT",
        "MANAGEMENT; SUPPLY CHN; Michigan State University": "SCM; MGT",
        "MANAGEMENT; Michigan State University; SUPPLY CHN": "SCM; MGT",
        "Michigan State University; MANAGEMENT; SUPPLY CHN": "SCM; MGT",
        "Michigan State University; SUPPLY CHN; MANAGEMENT": "SCM; MGT",

    }
    df['Department name'] = df['Department name'].replace(replacements)
    return df

# Step 4: Separate datasets
def separate_datasets(df):
    merged = df[df['Course code'].str.contains('MERGED', na=False) & (df['Total files'] != 0)]
    not_merged = df[~df['Course code'].str.contains('MERGED', na=False)]
    return merged, not_merged

# Step 5: Create comparative course codes
def create_comparative_course_codes(df):
    df = df[df['Course code'].notna() & df['Course code'].str.startswith(('SS', 'US', 'FS'))]

    def process_code(code):
        parts = code.split('-')
        return '-'.join(parts[1:4]) if len(parts) > 4 else code

    df.loc[:, 'Comparative Code'] = df['Course code'].apply(process_code)
    return df

# Step 6: Handle merged course codes
def process_merged_codes(merged, not_merged):
    # Helper function to extract course codes from merged entries
    def extract_course_codes(merged_code):
        components = merged_code.split('-')[1:]  # Skip the first 'MERGED' part
        return [component for component in components if len(component) >= 2]

    # Helper function to derive Merged Comparative Course Code
    def derive_merged_code(matching_codes):
        # Extract prefix-course pairs (e.g., SCM-474, MGT-231)
        prefix_course_pairs = sorted(set(f"{code.split('-')[0]}-{code.split('-')[1]}" for code in matching_codes))

        # Combine pairs with `/` separator
        merged_part = '-'.join(prefix_course_pairs)

        # Return the final merged code
        return f"{merged_part} MERGED"

    # Initialize list to store results
    merged_comparative_codes = []

    for _, row in merged.iterrows():
        # Extract component codes from the merged course code
        component_codes = extract_course_codes(row['Course code'])

        # Find matching Comparative Course Codes in not_merged
        matching_rows = not_merged[not_merged['Course code'].apply(
            lambda x: any(component in x for component in component_codes)
        )]
        matching_codes = matching_rows['Comparative Code'].tolist()

        # Derive the Merged Comparative Course Code
        if matching_codes:
            merged_code = derive_merged_code(matching_codes)
        else:
            merged_code = "UNKNOWN MERGED"  # Handle cases where no matches are found

        # Append the merged code to the list
        merged_comparative_codes.append(merged_code)

    # Add the new column to merged
    merged['Comparative Code'] = merged_comparative_codes
    return merged

# Step 7: Output separate CSVs by department
def output_department_csvs(df):
    departments = df['Department name'].unique()
    for dept in departments:
        dept_df = df[df['Department name'] == dept]
        dept_df.to_csv(f"{output_dir}{dept}.csv", index=False)

# Step 8: Filter 1-2 most recent instances of each course by Comparative Code
def filter_most_recent_instances(df):
    # Use the index of relevant_semesters for sorting
    df['Term Sort Key'] = df['Term name'].apply(lambda term: relevant_semesters.index(term))

    # Sort by Comparative Code and Term Sort Key
    df = df.sort_values(by=['Comparative Code', 'Term Sort Key'], ascending=[True, False])

    # Process each group
    filtered_rows = []
    for comparative_code, group in df.groupby('Comparative Code'):
        # Take the most recent 2 instances
        top_rows = group.head(2)

        if len(top_rows) == 2:
            # Compare Total Files and Overall Scores for the top 2 rows
            total_files_diff = abs(top_rows.iloc[0]['Total files'] - top_rows.iloc[1]['Total files'])
            overall_score_diff = abs(top_rows.iloc[0]['Overall score'] - top_rows.iloc[1]['Overall score'])

            # If differences are less than 10%, keep only the most recent row
            if (total_files_diff / max(top_rows.iloc[0]['Total files'], 1) < 0.1 and
                overall_score_diff / max(top_rows.iloc[0]['Overall score'], 1) < 0.1):
                filtered_rows.append(top_rows.iloc[0])
            else:
                filtered_rows.extend(top_rows.to_dict('records'))
        else:
            # If only 1 row exists, keep it
            filtered_rows.extend(top_rows.to_dict('records'))

    # Combine filtered rows into a final DataFrame
    final_df = pd.DataFrame(filtered_rows)
    return final_df.drop(columns=['Term Sort Key'])

# Step 9: Output separate CSVs by department FOR FILTERED
def output_department_csvs(df):
    departments = df['Department name'].unique()
    for dept in departments:
        dept_df = df[df['Department name'] == dept]
        dept_df.to_csv(f"{output_dir}{dept}_Most_Recent.csv", index=False)


# Main workflow
data = filter_semesters(data)

merged, not_merged = separate_datasets(data)

merged = filter_departments(merged)

not_merged = create_comparative_course_codes(not_merged)
merged = process_merged_codes(merged, not_merged)

not_merged = filter_departments(not_merged)
not_merged = not_merged[(not_merged['Total files'] != 0)]

merged = clean_department_names(merged)
not_merged = clean_department_names(not_merged)

combined_df = pd.concat([merged, not_merged])
filtered_df = filter_most_recent_instances(combined_df)

# Output final CSVs
#output_department_csvs(merged)
output_dir=f'output/'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_department_csvs(combined_df)
combined_df.to_csv(f"{output_dir}Broad_Ally_Report.csv", index=False)
#filtered_df.to_csv(f"{output_dir}Broad_Ally_Report_Filtered.csv", index=False)
#output_department_csvs(filtered_df)

#print("Processing complete.")