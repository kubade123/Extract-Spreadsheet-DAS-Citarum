import pandas as pd

sheet_id = '1ihz4Kf7caGd0Gsyceze-7Rr6t9mmFDPUfSbbnzvGKxM'  # Extracted from the URL
sheet_name = 'Sheet1'  # Check the name on the Google Sheets tab

url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'

# Read the sheet data into a pandas DataFrame
df = pd.read_csv(url)
print(df)
