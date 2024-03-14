import pandas as pd

def load_data(file_path, encoding_list=['utf-8', 'ISO-8859-1']):
    for encoding in encoding_list:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Failed to load the file {file_path} with provided encodings.")

def validate_standings_order(group):
    sorted_group = group.sort_values(by=['PuntenVoor', 'AantalGewonnen', 'Doelpuntensaldo', 'DoelpuntenVoor'], 
                                            ascending=[False, False, False, False])
    
    group['CorrectStand'] = sorted_group['Stand'].values
    group['StandCorrect'] = (group['Stand'] == group['CorrectStand'])

    return group

def print_errors(data, error_type):
    error_counts = data.groupby(['SeizoensBegin', 'Speeldag', 'Club']).size().reset_index(name='AantalFouten')
    error_counts['FoutType'] = error_type

    return error_counts

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

    # Uniek stamnmmer per club
    data['UniekeStamnummerPerClub'] = data.duplicated(subset=['Club', 'Stamnummer'], keep=False)

    # Consistentie SeizoensBegin en SeizoensEinde
    data['ConsistentSeizoen'] = data['SeizoensEinde'] > data['SeizoensBegin']

    # Correct aantal gespeelde wedstrijden (AantalGespeeld = AantalGewonnen + AantalGelijk + AantalVerloren)
    data['AantalGespeeld'] == (data['AantalGewonnen'] + data['AantalGelijk'] + data['AantalVerloren'])

    errors = pd.DataFrame()

    # Fouten rapporteren
    for condition, error_type in [
        (data['GeenEnkelRecordMeerWedstrijdenDanSpeeldagen'], 'MEER WEDSTRIJDEN DAN SPEELDAGEN'),
        (data['CorrectDoelpuntensaldo'], 'DOELPUNTENSALDO INCORRECT'),
        (data['CorrectVerwachtePunten'], 'VERWACHTE PUNTEN INCORRECT'),
        (data['UniekeStamnummerPerClub'], 'GEEN UNIEKE STAMNUMMER PER CLUB'),
        (data['ConsistentSeizoen'], 'INCONSISTENT SEIZOEN'),
        (data['AantalGespeeld'], 'AANTAL GESPEELDE WEDSTRIJDEN INCORRECT'),
    ]:
    
        errors = pd.concat([errors, print_errors(data[~condition], error_type)])

    validated_data = data.groupby(['SeizoensBegin', 'Speeldag']).apply(validate_standings_order).reset_index(drop=True)
    validation_errors = print_errors(validated_data[~validated_data['StandCorrect']], 'KLASSEMENT INCORRECT')

    errors = pd.concat([errors, validation_errors])

    return validated_data, errors

file_path_cleaned_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
file_path_controlled_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv'
file_path_errors_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\data_errors\errors_stand.csv'

controlled_data, errors = control_data(file_path_cleaned_data)
controlled_data.to_csv(file_path_controlled_data, index=False)
errors.to_csv(file_path_errors_data, index=False)
