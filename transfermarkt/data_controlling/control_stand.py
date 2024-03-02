import pandas as pd

def control_data(file_path):
    # Load the CSV file
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')

    # Calculate the difference between 'DoelpuntenVoor' and 'DoelpuntenTegen'
    # and check if it matches 'Doelpuntensaldo'
    data['DoelpuntenVoor'] = data['DoelpuntenVoor'].astype(int)  # Ensure the data type is integer
    data['DoelpuntenTegen'] = data['DoelpuntenTegen'].astype(int)
    data['Doelpuntensaldo'] = data['Doelpuntensaldo'].astype(int)
    data['DoelpuntensaldoCheck'] = (data['DoelpuntenVoor'] - data['DoelpuntenTegen']) == data['Doelpuntensaldo']
    
    # Filter out rows where 'DoelpuntensaldoCheck' is False
    incorrect_rows = data[~data['DoelpuntensaldoCheck']]
    
    # Check if there are any incorrect rows
    if not incorrect_rows.empty:
        print("Incorrect rows (DoelpuntensaldoCheck failed):")
        for index, row in incorrect_rows.iterrows():
            print(row)
    else:
        print("All rows passed the DoelpuntensaldoCheck.")

    return data

# Example usage, replace 'file_path' with the actual path to your cleaned CSV file
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
controlled_data = control_data(file_path)

# Save the controlled data to a new CSV
controlled_data.to_csv('stand_controlled.csv', index=False)