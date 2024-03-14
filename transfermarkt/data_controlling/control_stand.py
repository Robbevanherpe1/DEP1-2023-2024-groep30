import pandas as pd

# Laden van de gegevens uit een csv-bestand 
def load_data(file_path, encoding_list=['utf-8', 'ISO-8859-1']):
    for encoding in encoding_list:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Kon het bestand {file_path} niet laden met de opgegeven coderingen.")

def control_data(file_path):
    data = load_data(file_path)
    
    # Stel puntensysteem in op basis van het beginjaar
    jaarTallen2puntensysteem = set(range(1960, 1995)) - {1964}
    data['PuntenVoorOverwinning'] = data['SeizoensBegin'].apply(lambda x: 2 if x in jaarTallen2puntensysteem else 3)
    
    # Bereken verwachte punten op basis van winsten en gelijke spelen.
    data['VerwachtePunten'] = data['AantalGewonnen'] * data['PuntenVoorOverwinning'] + data['AantalGelijk']
    
    errors = pd.DataFrame()
    
    # Definieer condities voor verschillende validatiechecks.
    conditions = {
        'MeerWedstrijdenDanSpeeldagen': data['Speeldag'] >= data['AantalGespeeld'],
        'CorrectDoelpuntensaldo': (data['DoelpuntenVoor'] - data['DoelpuntenTegen']) == data['Doelpuntensaldo'],
        'CorrectVerwachtePunten': data['VerwachtePunten'] == data['PuntenVoor'],
        'ConsistentSeizoen': data['SeizoensEinde'] > data['SeizoensBegin'],
        'AantalGespeeldCorrect': data['AantalGespeeld'] == (data['AantalGewonnen'] + data['AantalGelijk'] + data['AantalVerloren'])
    }
    
    # Doorloop condities om fouten te detecteren en vast te leggen.
    for error_type, condition in conditions.items():
        error_data = data[~condition]
        errors = pd.concat([errors, error_data.groupby(['SeizoensBegin', 'Speeldag', 'Club']).size().reset_index(name='AantalFouten').assign(FoutType=error_type)])
    
    # Corrigeer stand en controleer op discrepanties.
    data = data.assign(
        CorrectStand=data.groupby(['SeizoensBegin', 'Speeldag']).cumcount() + 1,
        StandCorrect=lambda x: x['Stand'] == x['CorrectStand']
    )
    
    # Registreer fouten gerelateerd aan onjuiste stand.
    validation_errors = data[~data['StandCorrect']].groupby(['SeizoensBegin', 'Speeldag', 'Club']).size().reset_index(name='AantalFouten').assign(FoutType='KLASSEMENT INCORRECT')
    errors = pd.concat([errors, validation_errors])

    # Lijst van kolommen om te verwijderen uit de uiteindelijke dataset.
    columns_to_remove = ['ConsistentSeizoen','AantalGespeeldCorrect', 'PuntenVoorOverwinning', 'MeerWedstrijdenDanSpeeldagen', 'VerwachtePunten', 'CorrectDoelpuntensaldo', 'CorrectVerwachtePunten', 'CorrectStand', 'StandCorrect']
    
    # Retourneer de opgeschoonde en gevalideerde gegevens, samen met een DataFrame van fouten.
    return data.drop(columns=columns_to_remove, errors='ignore'), errors

# Bestandspaden voor csv-bestanden
file_path_cleaned_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
file_path_controlled_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv'
file_path_errors_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\data_errors\errors_stand.csv'

# Uitvoeren van de functies
controlled_data, errors = control_data(file_path_cleaned_data)
controlled_data.to_csv(file_path_controlled_data, index=False)
errors.to_csv(file_path_errors_data, index=False)
