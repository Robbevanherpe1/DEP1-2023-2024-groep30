import pandas as pd
from datetime import datetime
import pyodbc
from tqdm import tqdm

def connect_to_sqlserver():
    try:
        return pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DEP_DWH_G30;UID=sa;PWD=VMdepgroup30')
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        return None


def load_data_to_sqlserver(data, table_name, column_mapping, cnxn):
    if cnxn:
        try:
            cursor = cnxn.cursor()
            columns = ', '.join(column_mapping.values())
            placeholders = ', '.join(['?'] * len(column_mapping))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            # Prepare data as a list of tuples for the executemany method
            data_tuples = [tuple(row[key] for key in column_mapping.keys()) for _, row in data.iterrows()]
            # Use executemany for bulk insert
            cursor.executemany(query, data_tuples)
            cnxn.commit()
        except pyodbc.DatabaseError as db_err:
            print(f"Database error while loading data into {table_name}: {db_err}")
        except Exception as e:
            print(f"Unexpected error while loading data into {table_name}: {e}")
    else:
        print("Connection to SQL Server is not established.")


def calculate_date_fields(datum_str):
    datum_obj = datetime.strptime(datum_str, '%Y-%m-%d')
    return {
        'VolledigeDatumAlternatieveSleutel': datum_obj.strftime('%Y-%m-%d'),
        'Datum': datum_str,
        'DagVanDeMaand': datum_obj.day,
        'DagVanHetJaar': datum_obj.timetuple().tm_yday,
        'WeekVanHetJaar': datum_obj.isocalendar()[1],
        'DagVanDeWeekInMaand': (datum_obj.day - 1) // 7 + 1,
        'DagVanDeWeekInJaar': datum_obj.isocalendar()[2],
        'Maand': datum_obj.month,
        'Kwartaal': (datum_obj.month - 1) // 3 + 1,
        'Jaar': datum_obj.year,
        'EngelseDag': datum_obj.strftime('%A'),
        'EngelseMaand': datum_obj.strftime('%B'),
        'EngelsJaar': datum_obj.strftime('%Y'),
        'DDMMJJJJ': datum_obj.strftime('%d%m%Y')
    }


def calculate_time_fields(time_str):
    # Parse the input time string (expected format "HH:MM")
    time_obj = datetime.strptime(time_str, '%H:%M')
    
    return {
        'VolledigeTijdAlternatieveSleutel': time_obj.strftime('%H:%M'),
        'Uur': time_obj.hour,
        'Minuten': time_obj.minute,
        'VolledigeTijd': time_obj.strftime('%H%M')
    }

def process_and_load_csv(csv_path, cnxn):
    df = pd.read_csv(csv_path)
    if 'Datum' in df.columns:
        df = pd.concat([df.drop(columns=['Datum']), df['Datum'].apply(calculate_date_fields).apply(pd.DataFrame)], axis=1)
    
    # Nieuw: Voor Tijdstip
    if 'Tijdstip' in df.columns:
        df = pd.concat([df.drop(columns=['Tijdstip']), df['Tijdstip'].apply(calculate_time_fields).apply(pd.Series)], axis=1)
    
    mappings = {
        'DimTeam': {'Stamnummer': 'Stamnummer', 'RoepNaam': 'PloegNaam'},

        'DimDate': {k: k for k in calculate_date_fields('Datum').keys()},

        'DimTime': {k: k for k in calculate_time_fields('Tijdstip').keys()},

        'DimWedstrijd': {'Id': 'MatchID'},

        'DimKans': {'OddsWaarde': 'OddsWaarde'},

        'FactWedstrijdScore': {'TeamKeyUit': 'TeamKeyUit',
                               'TeamKeyThuis': 'TeamKeyThuis',
                               'WedstrijdKey': 'WedstrijdKey',
                               'DateKey': 'DateKey',
                               'TimeKey': 'TimeKey',
                               'ScoreThuis': 'ScoreThuis',
                               'ScoreUit': 'ScoreUit',
                               'FinaleStandThuisploeg': 'EindscoreThuis',
                               'FinaleStandUitploeg': 'EindscoreUit',
                               'RoepnaamScorendePloeg': 'ScorendePloegKey'},

        'FactWeddenschap': {'TeamKeyUit': 'TeamKeyUit',
                            'TeamKeyThuis': 'TeamKeyThuis',
                            'WedstrijdKey': 'WedstrijdKey',
                            'KansKey': 'KansKey',
                            'DateKeyScrape': 'DateKeyScrape',
                            'TimeKeyScrape': 'TimeKeyScrape',
                            'DateKeySpeeldatum': 'DateKeySpeeldatum',
                            'TimeKeySpeeldatum': 'TimeKeySpeeldatum',
                            'OddsThuisWint': 'OddsThuisWint',
                            'OddsUitWint': 'OddsUitWint',
                            'OddsGelijk': 'OddsGelijk',
                            'OddsBeideTeamsScoren': 'OddsBeideTeamsScoren',
                            'OddsNietBeideTeamsScoren': 'OddsNietBeideTeamsScoren',
                            'OddsMeerDanXGoals': 'OddsMeerDanXGoals',
                            'OddsMinderDanXGoals': 'OddsMinderDanXGoals'},

        'FactKlassement': {'BeginDateKey': 'BeginDateKey',
                           'EindeDateKey': 'EindeDateKey',
                           'TeamKey': 'TeamKey', 'Stand': 'Stand',
                           'AantalGespeeld': 'AantalGespeeld',
                           'AantalGewonnen': 'AantalGewonnen',
                           'AantalGelijk': 'AantalGelijk',
                           'AantalVerloren': 'AantalVerloren',
                           'DoelpuntenVoor': 'DoelpuntenVoor',
                           'DoelpuntenTegen': 'DoelpuntenTegen',
                           'DoelpuntenSaldo': 'DoelpuntenSaldo',
                           'PuntenVoor': 'PuntenVoor',
                           'PuntenTegen': 'PuntenTegen'}
    }

    for table_name, mapping in mappings.items():
        load_data_to_sqlserver(df, table_name, mapping, cnxn)


def main():
    try:
        cnxn = connect_to_sqlserver()
        if not cnxn:
            print("Failed to connect to SQL Server.")
            return

        csv_paths = [
            '/home/vicuser/data/klassementCorrect.csv', 
            '/home/vicuser/data/wedstrijdenCorrect.csv', 
            '/home/vicuser/data/doelpuntenCorrect.csv', 
            '/home/vicuser/data/betsCorrect.csv'
        ]

        for path in csv_paths:
            try:
                process_and_load_csv(path, cnxn)
            except Exception as e:
                print(f"Error processing file {path}: {e}")

        print("Data loading complete.")
    except Exception as e:
        print(f"Unexpected error in main function: {e}")
    finally:
        if cnxn:
            cnxn.close()

if __name__ == '__main__':
    main()