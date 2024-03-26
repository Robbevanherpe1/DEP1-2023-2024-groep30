#!/usr/bin/env python

import pandas as pd
import logging
from sqlalchemy import create_engine
from tqdm import tqdm
from datetime import datetime

# Stel het logniveau in
logging.basicConfig(level=logging.INFO)

def connect_to_sqlserver():
    try:
        # Verplaats informatie naar veilige locatie 
        connection_string = 'mssql+pyodbc://sa:VMdepgroup30@localhost/DEP_DWH_G30?driver=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes'
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        logging.error(f"Error connecting to SQL Server: {e}")
        return None

def process_dates(df, date_column):
    # Verwerk en voeg extra datum informatie toe aan datumkolommen 
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df['VolledigeDatumAlternatieveSleutel'] = df[date_column].dt.strftime('%Y-%m-%d')
    df['DagVanDeMaand'] = df[date_column].dt.day
    df['DagVanHetJaar'] = df[date_column].dt.dayofyear
    df['WeekVanHetJaar'] = df[date_column].dt.isocalendar().week
    df['DagVanDeWeekInMaand'] = (df[date_column].dt.day - 1) // 7 + 1
    df['DagVanDeWeekInJaar'] = df[date_column].dt.dayofweek + 1
    df['Maand'] = df[date_column].dt.month
    df['Kwartaal'] = (df[date_column].dt.month - 1) // 3 + 1
    df['Jaar'] = df[date_column].dt.year
    df['EngelseDag'] = df[date_column].dt.day_name()
    df['EngelseMaand'] = df[date_column].dt.month_name()
    return df

def process_times(df, time_column):
    # tijd in 'HH:MM' formaat
    df[time_column] = pd.to_datetime(df[time_column], format='%H:%M', errors='coerce').dt.time
    return df

def process_and_load_csv(csv_path, engine, table_name, date_column='Datum', time_column='Tijdstip'):
    df = pd.read_csv(csv_path, sep=';')
    if date_column in df.columns:
        df = process_dates(df, date_column)
    if time_column in df.columns:
        df = process_times(df, time_column)
    
     # Filter of transformeer de DataFrame op basis van de behoeften van elke tabel, laad vervolgens
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

def main():
    engine = connect_to_sqlserver()
    if not engine:
        logging.error("Failed to connect to SQL Server.")
        return

    csv_paths_table_mappings = {
        '/home/vicuser/data/klassementCorrect.csv', 
        '/home/vicuser/data/wedstrijdenCorrect.csv', 
        '/home/vicuser/data/doelpuntenCorrect.csv', 
        '/home/vicuser/data/betsCorrect.csv'
    }

    for path, table_name in tqdm(csv_paths_table_mappings.items(), desc="Processing CSV files"):
        try:
            process_and_load_csv(path, engine, table_name)
        except Exception as e:
            logging.error(f"Error processing file {path}: {e}")

    logging.info("Data loading complete.")

if __name__ == '__main__':
    main()
