import pandas as pd

def control_data(file_path):
    # Load the CSV file with a specified encoding to handle non-UTF-8 characters
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')
    
    # control data
    
    return data

# Example usage
file_path = r'DEP-G30\transfermarkt\data\controlled_data_stamnummer\matches_stamnummer.csv'
cleaned_data = control_data(file_path)

# Save the controlled data to a new CSV
cleaned_data.to_csv(r'DEP-G30\transfermarkt\data\controlled_data\matches_controlled.csv', index=False)