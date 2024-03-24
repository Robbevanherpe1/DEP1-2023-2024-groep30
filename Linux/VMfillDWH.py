import pandas as pd
from datetime import datetime
import pyodbc

def connect_to_sqlserver():
    server = 'localhost'
    database = 'DEP_DWH_G30'
    username = 'sa'
    password = 'VMdepgroup30'
    try:
        cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
        return cnxn
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        return None

def load_data_to_sqlserver(data, table_name, column_mapping, cnxn):
    if cnxn is None:
        print("Connection to SQL Server is not established.")
        return
    try:
        with cnxn.cursor() as cursor:
            columns = ', '.join(column_mapping.values())
            placeholders = ', '.join(['?'] * len(column_mapping))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            for _, row in data.iterrows():
                values = tuple(row[key] for key in column_mapping.keys())
                cursor.execute(query, values)
            cnxn.commit()
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

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
        'EngelseJaar': datum_obj.strftime('%Y'),
        'DDMMJJJJ': datum_obj.strftime('%d%m%Y')
    }

def process_and_load_csv(csv_path, cnxn):
    df = pd.read_csv(csv_path)
    if 'Datum' in df:
        transformed_dates = df['Datum'].apply(calculate_date_fields).apply(pd.Series)
        df = pd.concat([df.drop(columns=['Datum']), transformed_dates], axis=1)
    
    # Linkse waarden zijn van de CSV, rechtse waarden zijn de kolommen in de DWH-tabellen
    # Mapping voor DimTeam
    dim_team_mapping = {
        'Stamnummer': 'Stamnummer',
        'RoepNaam': 'PloegNaam'
    }
    # Aangepaste mapping voor DimDate die overeenkomt met de berekende datumvelden
    dim_date_mapping = {
        'VolledigeDatumAlternatieveSleutel': 'VolledigeDatumAlternatieveSleutel',
        'DagVanDeMaand': 'DagVanDeMaand',
        'DagVanHetJaar': 'DagVanHetJaar',
        'WeekVanHetJaar': 'WeekVanHetJaar',
        'DagVanDeWeekInMaand': 'DagVanDeWeekInMaand',
        'DagVanDeWeekInJaar': 'DagVanDeWeekInJaar',
        'Maand': 'Maand',
        'Kwartaal': 'Kwartaal',
        'Jaar': 'Jaar',
        'EngelseDag': 'EngelseDag',
        'EngelseMaand': 'EngelseMaand',
        'EngelsJaar': 'EngelseJaar',
        'DDMMJJJJ': 'DDMMJJJJ'
    }

    # Mapping voor DimTime
    dim_time_mapping = {
        'Uur': 'Uur',
        'Minuten': 'Minuten',
        'VolledigeTijd': 'VolledigeTijd'
    }

    # Mapping voor DimWedstrijd
    dim_wedstrijd_mapping = {
        'Id': 'MatchID'
    }

    # Mapping voor DimKans
    dim_kans_mapping = {
        'OddsWaarde': 'OddsWaarde'
    }

    # Mapping voor FactWedstrijdScore
    fact_wedstrijd_score_mapping = {
        'TeamKeyUit': 'TeamKeyUit',
        'TeamKeyThuis': 'TeamKeyThuis',
        'WedstrijdKey': 'WedstrijdKey',
        'DateKey': 'DateKey',
        'TimeKey': 'TimeKey',
        'ScoreThuis': 'ScoreThuis',
        'ScoreUit': 'ScoreUit',
        'FinaleStandThuisploeg': 'EindscoreThuis',
        'FinaleStandUitploeg': 'EindscoreUit',
        'RoepnaamScorendePloeg': 'ScorendePloegKey'
    }

    # Mapping voor FactWeddenschap
    fact_weddenschap_mapping = {
        'TeamKeyUit': 'TeamKeyUit',
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
        'OddsMinderDanXGoals': 'OddsMinderDanXGoals'
    }

    # Mapping voor FactKlassement
    fact_klassement_mapping = {
        'BeginDateKey': 'BeginDateKey',
        'EindeDateKey': 'EindeDateKey',
        'TeamKey': 'TeamKey',
        'Stand': 'Stand',
        'AantalGespeeld': 'AantalGespeeld',
        'AantalGewonnen': 'AantalGewonnen',
        'AantalGelijk': 'AantalGelijk',
        'AantalVerloren': 'AantalVerloren',
        'DoelpuntenVoor': 'DoelpuntenVoor',
        'DoelpuntenTegen': 'DoelpuntenTegen',
        'DoelpuntenSaldo': 'DoelpuntenSaldo',
        'PuntenVoor': 'PuntenVoor',
        'PuntenTegen': 'PuntenTegen'
    }

    try:
        load_data_to_sqlserver(df, 'DimTeam', dim_team_mapping, cnxn)
        load_data_to_sqlserver(transformed_dates.drop_duplicates(subset=['Datum']), 'DimDate', dim_date_mapping, cnxn)
        load_data_to_sqlserver(df, 'DimTime', dim_time_mapping, cnxn)
        load_data_to_sqlserver(df, 'DimWedstrijd', dim_wedstrijd_mapping, cnxn)
        load_data_to_sqlserver(df, 'DimKans', dim_kans_mapping, cnxn)
        load_data_to_sqlserver(df, 'FactWedstrijdScore', fact_wedstrijd_score_mapping, cnxn)
        load_data_to_sqlserver(df, 'FactWeddenschap', fact_weddenschap_mapping, cnxn)
        load_data_to_sqlserver(df, 'FactKlassement', fact_klassement_mapping, cnxn)
        pass
    except Exception as e:
        print(f"Error processing file {csv_path}: {e}")

def main():
    cnxn = connect_to_sqlserver()
    if cnxn is None:
        return

    csv_paths = [
        '/home/vicuser/data/klassementCorrect.csv', 
        '/home/vicuser/data/wedstrijdenCorrect.csv', 
        '/home/vicuser/data/doelpuntenCorrect.csv', 
        '/home/vicuser/data/bets.csv'
    ]

    for path in csv_paths:
        process_and_load_csv(path, cnxn)

    cnxn.close()

if __name__ == '__main__':
    main()