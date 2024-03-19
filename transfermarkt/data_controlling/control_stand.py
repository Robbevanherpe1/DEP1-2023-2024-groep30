import pandas as pd

# Functie om gegevens te laden uit een CSV-bestand met gespecificeerde coderingen
def load_data(file_path, encoding_list=['utf-8', 'ISO-8859-1']):
    for encoding in encoding_list:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Kon het bestand {file_path} niet laden met de opgegeven coderingen.")

# Functie om gegevens te controleren en te valideren
def control_data(file_path):
    data = load_data(file_path)

    # Zet de betreffende kolommen om naar numerieke typen, forceer niet-numerieke waarden naar NaN
    data['DoelpuntenVoor'] = pd.to_numeric(data['DoelpuntenVoor'], errors='coerce')
    data['DoelpuntenTegen'] = pd.to_numeric(data['DoelpuntenTegen'], errors='coerce')
    data['Doelpuntensaldo'] = pd.to_numeric(data['Doelpuntensaldo'], errors='coerce')
    data['Seizoen'] = pd.to_numeric(data['Seizoen'], errors='coerce')

    errors = pd.DataFrame()

    # Bereken punten op basis van het driepuntensysteem voor wedstrijden na 1995
    data.loc[data['Seizoen'] >= 1995, 'CalculatedPunten'] = data.loc[data['Seizoen'] >= 1995].apply(lambda row: 3 if row['AantalGewonnen'] > row['AantalVerloren'] else (1 if row['AantalGewonnen'] == row['AantalVerloren'] else 0), axis=1)

    # Bereken punten voor wedstrijden voor 1995
    data.loc[data['Seizoen'] < 1995, 'CalculatedPunten'] = data.loc[data['Seizoen'] < 1995].apply(lambda row: 2 if row['AantalGewonnen'] > row['AantalVerloren'] else (1 if row['AantalGewonnen'] == row['AantalVerloren'] else 0), axis=1)

    # Controleer of de puntenkolom overeenkomt met de berekende punten
    points_mismatch = data[data['Doelpuntensaldo'] != data['CalculatedPunten']]
    if not points_mismatch.empty:
        errors = pd.concat([errors, points_mismatch.assign(FoutType='Punten kloppen niet')])

    # Bestaande condities checks
    conditions = {
        'MeerWedstrijdenDanSpeeldagen': data['Speeldag'] >= data['AantalGespeeld'],
        'CorrectDoelpuntensaldo': (data['DoelpuntenVoor'] - data['DoelpuntenTegen']) == data['Doelpuntensaldo'],
        'AantalGespeeldCorrect': data['AantalGespeeld'] == (data['AantalGewonnen'] + data['AantalGelijk'] + data['AantalVerloren']),
        'Links_TweepuntensysteemCorrect': data['Links_Tweepuntensysteem'] == (data['AantalGewonnen'] * 2 + data['AantalGelijk']),
        'Rechts_TweepuntensysteemCorrect': data['Rechts_Tweepuntensysteem'] == (data['AantalVerloren'] * 2 + data['AantalGelijk'])
    }

    # Controleer op fouten en registreer deze.
    for error_type, condition in conditions.items():
        error_data = data[~condition]
        errors = pd.concat([errors, error_data.groupby(['Seizoen', 'Speeldag', 'Roepnaam']).size().reset_index(name='AantalFouten').assign(FoutType=error_type)])

    # Corrigeer stand en controleer op fouten
    data = data.assign(
        CorrectStand=data.groupby(['Seizoen', 'Speeldag']).cumcount() + 1,
        StandCorrect=lambda x: x['Stand'] == x['CorrectStand']
    )

    # Registreer fouten gerelateerd aan onjuiste stand
    validation_errors = data[~data['StandCorrect']].groupby(['Seizoen', 'Speeldag', 'Roepnaam']).size().reset_index(name='AantalFouten').assign(FoutType='KLASSEMENT INCORRECT')
    errors = pd.concat([errors, validation_errors])

    # Lijst van kolommen om te verwijderen uit de uiteindelijke csv
    columns_to_remove = ['CorrectStand', 'StandCorrect']

    # Retourneer de opgeschoonde en gevalideerde gegevens, samen met een DataFrame van fouten
    return data.drop(columns=columns_to_remove, errors='ignore'), errors

# Bestandspaden voor csv-bestanden
file_path_cleaned_data = 'clean_stand.csv' # Corrected to be a file path string
file_path_doelpunten = 'clean_goals.csv' # Corrected to be a file path string
file_path_controlled_data = 'controlled_data.csv'
file_path_errors_data = 'errors_stand.csv'
# Uitvoeren van de functies
controlled_data, errors = control_data(file_path_cleaned_data)

# Opslaan van gecontroleerde data en errors
controlled_data.to_csv(file_path_controlled_data, index=False, header=False, sep=';')
errors.to_csv(file_path_errors_data, index=False)
