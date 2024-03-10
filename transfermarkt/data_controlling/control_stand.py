import pandas as pd

def validate_standings_order(group):
    # Sort the group based on the criteria
    sorted_group = group.sort_values(by=['PuntenVoor', 'AantalGewonnen', 'AantalGelijk', 'Doelpuntensaldo', 'DoelpuntenVoor'], ascending=[False, False, False, False, False])
    # Generate a new 'Stand' based on the correct order
    sorted_group['CorrectStand'] = range(1, len(sorted_group) + 1)
    # Check if the new 'Stand' matches the original 'Stand'
    group['StandCorrect'] = (sorted_group['Stand'] == sorted_group['CorrectStand'])
    return group

def load_data(file_path, encoding_list=['utf-8', 'ISO-8859-1']):
    for encoding in encoding_list:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Failed to load the file {file_path} with provided encodings.")

def control_data(file_path):
    # Load the CSV files
    data = load_data(file_path)
    
    # Set up the points system
    jaarTallen2puntensysteem = set(range(1960, 1995)) - {1964}
    data['PointsForWin'] = data['SeizoensBegin'].apply(lambda x: 2 if x in jaarTallen2puntensysteem else 3)

    # Calculate expected points
    data['ExpectedPoints'] = data['AantalGewonnen'] * data['PointsForWin'] + data['AantalGelijk']
    
    # Doelpuntensaldo check
    data['CorrectDoelpuntensaldo'] = (data['DoelpuntenVoor'] - data['DoelpuntenTegen']) == data['Doelpuntensaldo']
    
    # Expected points check
    data['CorrectExpectedPoints'] = data['ExpectedPoints'] == data['PuntenVoor']

    validated_data = data.groupby(['SeizoensBegin', 'Speeldag']).apply(validate_standings_order)
    
    # Find and report errors for Doelpuntensaldo and Verwachte Punten
    for error_type, error_df in [('Doelpuntensaldo', data[~data['CorrectDoelpuntensaldo']]), ('Verwachte Punten', data[~data['CorrectExpectedPoints']])]:
        if not error_df.empty:
            error_info = error_df[['SeizoensBegin', 'Speeldag']].drop_duplicates()
            for _, row in error_info.iterrows():
                print(f"Fouten gevonden in {error_type} voor Seizoen {row['SeizoensBegin']}, Speeldag {row['Speeldag']}: {len(error_df[(error_df['SeizoensBegin'] == row['SeizoensBegin']) & (error_df['Speeldag'] == row['Speeldag'])])} rijen")

    # Check if any incorrect standings were found and report them
    incorrect_standings = validated_data[~validated_data['StandCorrect']]
    if not incorrect_standings.empty:
        incorrect_info = incorrect_standings[['SeizoensBegin', 'Speeldag']].drop_duplicates()
        for _, row in incorrect_info.iterrows():
            print(f"Incorrect standings found in Season {row['SeizoensBegin']}, Matchday {row['Speeldag']}: {len(incorrect_standings[(incorrect_standings['SeizoensBegin'] == row['SeizoensBegin']) & (incorrect_standings['Speeldag'] == row['Speeldag'])])} rows.")
    else:
        print("All standings are correct.")
        
    return validated_data

# Path to your cleaned CSV file
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'

controlled_data = control_data(file_path)

# Save the controlled data to a new CSV
controlled_data.to_csv(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv', index=False)
