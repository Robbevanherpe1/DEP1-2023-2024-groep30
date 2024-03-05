import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, Date, Time, Float, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, scoped_session
import os
import logging

# Logging configureren
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Gebruik Environment Variabelen voor gevoelige informatie
db_user = os.getenv('DB_USER', 'root')
db_password = os.getenv('DB_PASSWORD', 'root')
db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', '3306')
db_name = os.getenv('DB_NAME', 'DEP_G30_DWH')

# Database connectie string
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
Session = scoped_session(sessionmaker(bind=engine))

# Aanmaken van een MetaData instantie
metadata = MetaData()

# Tabellen aanmaken
Fact_JPL = Table('Fact_JPL', metadata,
                  Column('JPLKey', Integer, primary_key=True),
                  Column('StandKey', Integer, ForeignKey('Dim_Stand.StandKey')),
                  Column('GoalKey', Integer, ForeignKey('Dim_Goal.GoalKey')),
                  Column('MatchKey', Integer, ForeignKey('Dim_Match.MatchKey')),
                  Column('WeddenschapKey', Integer, ForeignKey('Dim_Weddenschap.WeddenschapKey')),
                  )

Dim_Stand = Table('Dim_Stand', metadata,
                  Column('StandKey', Integer, primary_key=True, autoincrement=True),
                  Column('SeizoensBegin', Integer),
                  Column('SeizoensEinde', Integer),
                  Column('Speeldag', Integer),
                  Column('Stand', Integer),
                  Column('Club', String(255)),
                  Column('AantalGespeeld', Integer),
                  Column('AantalGewonnen', Integer),
                  Column('AantalGelijk', Integer),
                  Column('AantalVerloren', Integer),
                  Column('DoelpuntenVoor', Integer),
                  Column('DoelpuntenTegen', Integer),
                  Column('Doelpuntensaldo', Integer),
                  Column('PuntenVoor', Integer),
                  Column('PuntenTegen', Integer),
                  Column('Stamnummer', Integer),
                  )

Dim_Goal = Table('Dim_Goal', metadata,
                  Column('GoalKey', Integer, primary_key=True, autoincrement=True),
                  Column('Match_ID', Float),
                  Column('Seizoen', String(255)),
                  Column('Speeldag', Integer),
                  Column('Datum', Date),
                  Column('Tijdstip', Time),
                  Column('Thuisploeg', String(255)),
                  Column('Uitploeg', String(255)),
                  Column('NaamScorendePloeg', String(255)),
                  Column('GoalTijdstip', Time),
                  Column('GoalScorer', String(255)),
                  Column('StandThuisploeg', Integer),
                  Column('StandUitploeg', Integer),
                  Column('Thuisploeg_stamnummer', String(255)),
                  Column('Uitploeg_stamnummer', String(255)),
                  )

Dim_Match = Table('Dim_Match', metadata,
                    Column('MatchKey', Integer, primary_key=True, autoincrement=True),
                    Column('Match_ID', Integer),
                    Column('Seizoen', String(255)),
                    Column('Speeldag', Integer),
                    Column('Datum', Date),
                    Column('Tijdstip', Time),
                    Column('Thuisploeg', String(255)),
                    Column('Resultaat_Thuisploeg', Integer),
                    Column('Resultaat_Uitploeg', Integer),
                    Column('Uitploeg', String(255)),
                    Column('Thuisploeg_stamnummer', String(255)),
                    Column('Uitploeg_stamnummer', String(255)),
                    )

Dim_Weddenschap = Table('Dim_Weddenschap', metadata,
                        Column('WeddenschapKey', Integer, primary_key=True, autoincrement=True),
                        Column('ID', String(255)),
                        Column('Wedstrijd', String(255)),
                        Column('Starttijd', DateTime),
                        Column('Thuisploeg', String(255)),
                        Column('Uitploeg', String(255)),
                        Column('Vraag', String(255)),
                        Column('Keuze', String(255)),
                        Column('Kans', Float),
                        )

# Creëer alle tabellen in de database
metadata.create_all(engine)

# Functie om data te laden vanuit een lijst van CSV bestanden naar overeenstemmende tabellen
def load_data_from_csv_list(csv_file_paths):
    session = Session()
    try:
        for file_path, table_name in csv_file_paths:
            df = pd.read_csv(file_path)
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            logging.info(f"Data from {file_path} successfully loaded into {table_name}")
    except IOError as e:
        logging.error(f"File error: {e}")
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
    finally:
        session.close()

# Lijst van CSV bestanden en hun overeenstemmende tabelnamen
csv_file_paths = [
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data_stamnummer\stand_stamnummer.csv', 'Dim_Stand'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data_stamnummer\matches_stamnummer.csv', 'Dim_Match'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data_stamnummer\stand_stamnummer.csv', 'Dim_Goal'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\bet777\data\bets.csv', 'Dim_Weddenschap'),
]

# Laad data in vanuit de opgegeven CSV bestanden naar de overeenstemmende tabellen
load_data_from_csv_list(csv_file_paths)