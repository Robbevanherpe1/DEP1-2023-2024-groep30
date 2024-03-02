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
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_cleaned.csv'
cleaned_data = control_data(file_path)

# Save the controlled data to a new CSV
cleaned_data.to_csv('stand_cleaned.csv', index=False)
