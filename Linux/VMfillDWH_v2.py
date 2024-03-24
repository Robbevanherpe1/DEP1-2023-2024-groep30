import pandas as pd
from datetime import datetime
import pyodbc

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
            query = f"INSERT INTO {table_name} ({', '.join(column_mapping.values())}) VALUES ({', '.join(['?'] * len(column_mapping))})"
            for _, row in data.iterrows():
                cursor.execute(query, tuple(row[key] for key in column_mapping.keys()))
            cnxn.commit()
        except Exception as e:
            print(f"Error loading data into {table_name}: {e}")
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

def process_and_load_csv(csv_path, cnxn):
    df = pd.read_csv(csv_path)
    if 'Datum' in df.columns:
        df = pd.concat([df.drop(columns=['Datum']), df['Datum'].apply(calculate_date_fields).apply(pd.DataFrame)], axis=1)
    
    mappings = {
        'DimTeam': {'Stamnummer': 'Stamnummer', 'RoepNaam': 'PloegNaam'},
        'DimDate': {k: k for k in calculate_date_fields('2023-01-01').keys()},
        'DimTime': {'Uur': 'Uur', 'Minuten': 'Minuten', 'VolledigeTijd': 'VolledigeTijd'},
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
    cnxn = connect_to_sqlserver()
    if not cnxn:
        return

    csv_paths = [
        '/home/vicuser/data/klassementCorrect.csv', 
        '/home/vicuser/data/wedstrijdenCorrect.csv', 
        '/home/vicuser/data/doelpuntenCorrect.csv', 
        '/home/vicuser/data/bets.csv'
    ]

    for path in csv_paths:
        process_and_load_csv(path, cnxn)

    print("Data loading complete.")
    cnxn.close()

if __name__ == '__main__':
    main()