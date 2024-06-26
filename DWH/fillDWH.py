#!/usr/bin/env python

import pandas as pd
from datetime import datetime
import pyodbc
from tqdm import tqdm
from datetime import time
import logging
from dateutil.parser import parse

logging.basicConfig(level=logging.INFO)

def connect_to_sqlserver():
    try:
        return pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=DEP_DWH_G30;UID=sa;PWD=VMdepgroup30;TrustServerCertificate=yes')
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        return None


def load_data_to_sqlserver(data, table_name, column_mapping, cnxn):
    if cnxn:
        try:
            with cnxn.cursor() as cursor:
                columns = ', '.join([f'[{col}]' for col in column_mapping.values()])
                placeholders = ', '.join(['?'] * len(column_mapping))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                logging.info(f"Executing query: {query}")
                data_tuples = [tuple(row[key] for key in column_mapping.keys()) for _, row in data.iterrows()]
                cursor.executemany(query, data_tuples)
                cnxn.commit()
        except pyodbc.DatabaseError as db_err:
            logging.error(f"Database error while loading data into {table_name}: {db_err}")
        except Exception as e:
            logging.error(f"Unexpected error while loading data into {table_name}: {e}")
    else:
        logging.error("Connection to SQL Server is not established.")


def calculate_date_fields(datum_str):
    try:
        datum_obj = datetime.strptime(datum_str, '%Y-%m-%d').date()
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
    except ValueError:
        logging.error(f"Invalid date format for {datum_str}, expected 'YYYY-MM-DD'")
        return {}


def calculate_time_fields(time_str):
    try:
        tijd_obj = time.fromisoformat(time_str)
        return {
            'VolledigeTijdAlternatieveSleutel': tijd_obj.strftime('%H:%M'),
            'Uur': tijd_obj.hour,
            'Minuten': tijd_obj.minute,
            'VolledigeTijd': tijd_obj.strftime('%H%M')
        }
    except ValueError:
        logging.error(f"Invalid time format for {time_str}, expected 'HH:MM'")
        return {}
    

def process_and_load_csv(csv_path, cnxn):
    df = pd.read_csv(csv_path, sep=';')
    if 'Datum' in df.columns:
        date_fields_df = df['Datum'].apply(calculate_date_fields).apply(pd.Series)
        df = pd.concat([df.drop(columns=['Datum']), date_fields_df], axis=1)
    
    if 'Tijdstip' in df.columns:
        time_fields_df = df['Tijdstip'].apply(calculate_time_fields).apply(pd.Series)
        df = pd.concat([df.drop(columns=['Tijdstip']), time_fields_df], axis=1)

    # Mappings voor de kolomnamen, links zijn de kolomnamen in de CSV, rechts zijn de kolomnamen in de database
    mappings = {
        'DimTeam': {
            'StamnummerThuisploeg': 'Stamnummer',
            'RoepNaamThuisploeg': 'PloegNaam'
        },  

        'DimDate': {k: k for k in calculate_date_fields('Datum').keys()},

        'DimTime': {k: k for k in calculate_time_fields('Tijdstip').keys()},

        'DimWedstrijd': {
            'Id': 'MatchID'
        },

        'DimKans': {
            'OddsWaarde': 'OddsWaarde'
        },

        'FactWedstrijdScore': {
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
        },

        'FactWeddenschap': {
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
        },
        
        'FactKlassement': {
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
    }

    # Iterate over de mappings en laad de gegevens in SQL Server
    for table_name, mapping in mappings.items():
     try:
        load_data_to_sqlserver(df, table_name, mapping, cnxn)
     except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        
def main():
    try:
        cnxn = connect_to_sqlserver()
        if not cnxn:
            logging.error("Failed to connect to SQL Server.")
            return

        csv_paths = [
            '/home/vicuser/data/klassementCorrect.csv', 
            '/home/vicuser/data/wedstrijdenCorrect.csv', 
            '/home/vicuser/data/doelpuntenCorrect.csv', 
            '/home/vicuser/data/betsCorrect.csv'
        ]

        for path in tqdm(csv_paths, desc="Processing CSV files"):
            try:
                process_and_load_csv(path, cnxn)
            except Exception as e:
                logging.error(f"Error processing file {path}: {e}")

        logging.error("Data loading complete.")
    except Exception as e:
        logging.error(f"Unexpected error in main function: {e}")
    finally:
        if cnxn:
            cnxn.close()

if __name__ == '__main__':
    main()