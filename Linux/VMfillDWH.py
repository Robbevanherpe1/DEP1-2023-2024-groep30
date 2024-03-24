import pandas as pd
from datetime import datetime
import pyodbc

# Verbinding maken met SQL Server
def connect_to_sqlserver():
    server = 'localhost'
    database = 'DEP_DWH_G30'
    username = 'sa'
    password = 'VMdepgroup30'
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    
    return cnxn

# Functie om specifieke data van een CSV naar SQL Server te laden
def load_data_to_sqlserver(data, table_name, column_mapping, cnxn):
    cursor = cnxn.cursor()
    columns = ', '.join(column_mapping.values()) # Kolommen in de tabel
    placeholders = ', '.join(['?'] * len(column_mapping)) # Placeholders voor waarden
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    for _, row in data.iterrows():
        values = tuple(row[key] for key in column_mapping.keys())
        cursor.execute(query, values)

    cnxn.commit()
    cursor.close()


def calculate_date_fields(datum_str):
    datum_obj = datetime.strptime(datum_str, '%Y-%m-%d')
    return {
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

# Hoofdfunctie om CSV-data te verwerken en te laden
def process_and_load_csv(csv_path, cnxn):
    df = pd.read_csv(csv_path)

    # Linkse waarden zijn van de CSV, rechtse waarden zijn de kolommen in de DWH-tabellen
    # Mapping voor DimTeam
    dim_team_mapping = {
        'Stamnummer': 'Stamnummer',
        'RoepNaam': 'PloegNaam'
    }

    if 'Datum' in df:
        transformed_dates = df['Datum'].apply(calculate_date_fields).apply(pd.Series)
        # Voeg de berekende velden toe aan df voor andere doeleinden, indien nodig
        df = pd.concat([df.drop(columns=['Datum']), transformed_dates], axis=1)
    
    # Aangepaste mapping voor DimDate die overeenkomt met de berekende datumvelden
    dim_date_mapping = {
        'Datum': 'Datum',
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
        'EngelsJaar': 'EngelsJaar',
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

    load_data_to_sqlserver(df, 'DimTeam', dim_team_mapping, cnxn)
    load_data_to_sqlserver(transformed_dates.drop_duplicates(subset=['Datum']), 'DimDate', dim_date_mapping, cnxn)
    load_data_to_sqlserver(df, 'DimTime', dim_time_mapping, cnxn)
    load_data_to_sqlserver(df, 'DimWedstrijd', dim_wedstrijd_mapping, cnxn)
    load_data_to_sqlserver(df, 'DimKans', dim_kans_mapping, cnxn)
    load_data_to_sqlserver(df, 'FactWedstrijdScore', fact_wedstrijd_score_mapping, cnxn)
    load_data_to_sqlserver(df, 'FactWeddenschap', fact_weddenschap_mapping, cnxn)
    load_data_to_sqlserver(df, 'FactKlassement', fact_klassement_mapping, cnxn)

def main():
  cnxn = connect_to_sqlserver()

  # Pas paden aan naar je CSV-bestanden
  csv_paths = ['/home/vicuser/data/klassementCorrect.csv', '/home/vicuser/data/wedstrijdenCorrect.csv', 
               '/home/vicuser/data/doelpuntenCorrect.csv', '/home/vicuser/data/bets.csv']

  for path in csv_paths:
    process_and_load_csv(path, cnxn)

  cnxn.close()

if __name__ == '__main__':
    main()