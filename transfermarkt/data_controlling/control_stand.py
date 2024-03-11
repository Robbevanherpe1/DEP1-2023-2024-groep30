import pandas as pd

def validate_standings_order(group):
    # Sorteer de groep op basis van de opgegeven criteria
    sorted_group = group.sort_values(by=['PuntenVoor', 'AantalGewonnen', 'Doelpuntensaldo', 'DoelpuntenVoor'], 
                                     ascending=[False, False, False, False])
    # Genereer een nieuwe 'CorrectStand' gebaseerd op de juiste volgorde
    sorted_group['CorrectStand'] = range(1, len(sorted_group) + 1)
    # Check of de nieuwe 'CorrectStand' overeenkomt met de originele 'Stand'
    group['StandCorrect'] = (group['Stand'].values == sorted_group['CorrectStand'].values)
    # Houd de clubnamen bij tijdens het sorteren voor het geval we ze nodig hebben voor rapportage
    group['Club'] = sorted_group['Club']
    return group

def load_data(file_path, encoding_list=['utf-8', 'ISO-8859-1']):
    for encoding in encoding_list:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Failed to load the file {file_path} with provided encodings.")

def control_data(file_path):
    # Laad de CSV-bestanden
    data = load_data(file_path)
    
    # Stel het puntensysteem in
    jaarTallen2puntensysteem = set(range(1960, 1995)) - {1964}
    data['PointsForWin'] = data['SeizoensBegin'].apply(lambda x: 2 if x in jaarTallen2puntensysteem else 3)

    # Bereken verwachte punten
    data['ExpectedPoints'] = data['AantalGewonnen'] * data['PointsForWin'] + data['AantalGelijk']
    
    # Check op correcte doelpuntensaldo
    data['CorrectDoelpuntensaldo'] = (data['DoelpuntenVoor'] - data['DoelpuntenTegen']) == data['Doelpuntensaldo']
    
    # Check op correcte verwachte punten
    data['CorrectExpectedPoints'] = data['ExpectedPoints'] == data['PuntenVoor']

    validated_data = data.groupby(['SeizoensBegin', 'Speeldag']).apply(validate_standings_order)
    
    # Rapporteer fouten voor Doelpuntensaldo en Verwachte Punten
    for error_type, error_df in [('Doelpuntensaldo', data[~data['CorrectDoelpuntensaldo']]), ('Verwachte Punten', data[~data['CorrectExpectedPoints']])]:
        if not error_df.empty:
            error_info = error_df[['SeizoensBegin', 'Speeldag', 'Club']].drop_duplicates()
            for _, row in error_info.iterrows():
                print(f"Fouten gevonden in {error_type} voor Seizoen {row['SeizoensBegin']}, Speeldag {row['Speeldag']} voor club: {row['Club']}: {len(error_df[(error_df['SeizoensBegin'] == row['SeizoensBegin']) & (error_df['Speeldag'] == row['Speeldag'])])} rijen")

    # Controleer op incorrecte standen en rapporteer deze
    incorrect_standings = validated_data[~validated_data['StandCorrect']]
    if not incorrect_standings.empty:
        incorrect_info = incorrect_standings[['SeizoensBegin', 'Speeldag', 'Club']].drop_duplicates()
        for _, row in incorrect_info.iterrows():
            print(f"Incorrecte standen gevonden in Seizoen {row['SeizoensBegin']}, Speeldag {row['Speeldag']} voor club: {row['Club']}: {len(incorrect_standings[(incorrect_standings['SeizoensBegin'] == row['SeizoensBegin']) & (incorrect_standings['Speeldag'] == row['Speeldag'])])} rijen.")
    else:
        print("Alle standen zijn correct.")
        
    return validated_data


# Path to your cleaned CSV file
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'

controlled_data = control_data(file_path)

# Save the controlled data to a new CSV
controlled_data.to_csv(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv', index=False)
