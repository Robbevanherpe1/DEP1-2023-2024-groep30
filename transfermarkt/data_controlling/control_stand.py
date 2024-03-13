import pandas as pd

def load_data(file_path, encoding_list=['utf-8', 'ISO-8859-1']):
    for encoding in encoding_list:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Failed to load the file {file_path} with provided encodings.")


def validate_standings_order(group):
    # Sorteer de groep op basis van de opgegeven criteria
    sorted_group = group.sort_values(by=['PuntenVoor', 'AantalGewonnen', 'Doelpuntensaldo', 'DoelpuntenVoor'], 
                                     ascending=[False, False, False, False])
    
    # Genereer een nieuwe 'CorrectStand' gebaseerd op de juiste volgorde
    group['CorrectStand'] = sorted_group['Stand'].values

    # Check of de nieuwe 'CorrectStand' overeenkomt met de originele 'Stand'
    group['StandCorrect'] = (group['Stand'] == group['CorrectStand'])

    return group


def print_errors(data, error_type, condition):
    error_df = data[~condition]
    if not error_df.empty:
        error_info = error_df[['SeizoensBegin', 'Speeldag', 'Club']].drop_duplicates()
        for _, row in error_info.iterrows():
            error_count = len(error_df[(error_df['SeizoensBegin'] == row['SeizoensBegin']) & (error_df['Speeldag'] == row['Speeldag'])])
            print(f"Fouten gevonden in {error_type} voor Seizoen {row['SeizoensBegin']}, Speeldag {row['Speeldag']} voor club: {row['Club']}: {error_count} rijen")    


def control_data(file_path):
    # Laad de CSV-bestanden
    data = load_data(file_path)
    
    # Stel het puntensysteem in
    jaarTallen2puntensysteem = set(range(1960, 1995)) - {1964}
    data['PuntenVoorOverwinning'] = data['SeizoensBegin'].apply(lambda x: 2 if x in jaarTallen2puntensysteem else 3)

    # Geen enkel record met meer wedstrijden dan speeldagen
    data['GeenEnkelRecordMeerWedstrijdenDanSpeeldagen'] = data['Speeldag'] >= data['AantalGespeeld']

    # Bereken verwachte punten
    data['VerwachtePunten'] = data['AantalGewonnen'] * data['PuntenVoorOverwinning'] + data['AantalGelijk']
    
    # Check op correcte doelpuntensaldo
    data['CorrectDoelpuntensaldo'] = (data['DoelpuntenVoor'] - data['DoelpuntenTegen']) == data['Doelpuntensaldo']
    
    # Check op correcte verwachte punten
    data['CorrectVerwachtePunten'] = data['VerwachtePunten'] == data['PuntenVoor']
    
    # Fouten rapporteren
    print_errors(data, 'MEER WEDSTRIJDEN DAN SPEELDAGEN', data['GeenEnkelRecordMeerWedstrijdenDanSpeeldagen'])
    print_errors(data, 'DOELPUNTENSALDO INCORRECT', data['CorrectDoelpuntensaldo'])
    print_errors(data, 'VERWACHTE PUNTEN INCORRECT', data['CorrectVerwachtePunten'])

    validated_data = data.groupby(['SeizoensBegin', 'Speeldag']).apply(validate_standings_order)
    print_errors(validated_data, 'KLASSEMENT INCORRECT', validated_data['StandCorrect'])
  
    return validated_data


# pad naar stand_clean.csv
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
controlled_data = control_data(file_path)

# stand_controlled.csv opslaan
controlled_data.to_csv(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv', index=False)
