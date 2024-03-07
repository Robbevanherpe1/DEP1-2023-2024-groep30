import pandas as pd
from sqlalchemy import insert
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

# Dimensie tabellen
Dim_Ploeg = Table('Dim_Ploeg', metadata,
                  Column('PloegKey', Integer, primary_key=True),
                  Column('Id', String(255)),
                  Column('Stamnummer', String(255)),
                  Column('Naam', String(255)),
                  )

Dim_Date = Table('Dim_Date', metadata,
                 Column('DateKey', Integer, primary_key=True),
                 Column('Speeldag', Integer),
                 Column('Seizoen', String(255)),
                 Column('DagVanDeMaand', Integer),
                 Column('DagVanDeWeek', String(255)),
                 Column('DagVanHetJaar', Integer),
                 Column('WeekVanHetJaar', Integer),
                 Column('Maand', Integer),
                 Column('Jaar', Integer),
                 Column('DDMMJJJJ', String(255)),
                 )

Dim_Time = Table('Dim_Time', metadata,
                 Column('TimeKey', Integer, primary_key=True),
                 Column('Uur', Integer),
                 Column('Minuten', Integer),
                 Column('Seconden', Integer),
                 Column('AMofPM', String(255)),
                 Column('UurVanDeDag', String(255)),
                 Column('MinuutVanHetUur', Integer),
                 )

Dim_Klassement = Table('Dim_Klassement', metadata,
                       Column('KlassementKey', Integer, primary_key=True),
                       Column('Stand', Integer),
                       Column('AantalGespeeld', Integer),
                       Column('AantalGewonnen', Integer),
                       Column('AantalGelijk', Integer),
                       Column('AantalVerloren', Integer),
                       Column('DoelpuntenVoor', Integer),
                       Column('DoelpuntenTegen', Integer),
                       Column('DoelpuntenSaldo', Integer),
                       Column('PuntenVoor', Integer),
                       Column('PuntenTegen', Integer),
                       )

Dim_Match = Table('Dim_Match', metadata,
                  Column('MatchKey', Integer, primary_key=True),
                  Column('MatchID', Integer),
                  Column('Seizoen', String(255)),
                  Column('Speeldag', Integer),
                  Column('Datum', Date),
                  Column('Tijdstip', Time),
                  Column('Thuisploeg', String(255)),
                  Column('Uitploeg', String(255)),
                  Column('Resultaat_Thuisploeg', Integer),
                  Column('Resultaat_Uitploeg', Integer),
                  Column('Thuisploeg_stamnummer', String(255)),
                  Column('Uitploeg_stamnummer', String(255)),
                  )

Dim_Kansen = Table('Dim_Kansen', metadata,
                   Column('KansKey', Integer, primary_key=True),
                   Column('Waarde', Float, unique=True),
                   )

# Fact tabellen
Fact_Score = Table('Fact_Score', metadata,
                   Column('Id', Integer, primary_key=True),
                   Column('PloegKey', Integer, ForeignKey('Dim_Ploeg.PloegKey')),
                   Column('MatchKey', Integer, ForeignKey('Dim_Match.MatchKey')),
                   Column('KlassementKey', Integer, ForeignKey('Dim_Klassement.KlassementKey')),
                   Column('DateKey', Integer, ForeignKey('Dim_Date.DateKey')),
                   Column('TimeKey', Integer, ForeignKey('Dim_Time.TimeKey')),
                   Column('ScoreThuis', Integer),
                   Column('ScoreUit', Integer),
                   Column('EindscoreThuis', Integer),
                   Column('EindscoreUit', Integer),
                   Column('ScoreIndicator', String(255)),
                   )

Fact_Weddenschap = Table('Fact_Weddenschap', metadata,
                         Column('Id', Integer, primary_key=True),
                         Column('PloegKey', Integer, ForeignKey('Dim_Ploeg.PloegKey')),
                         Column('MatchKey', Integer, ForeignKey('Dim_Match.MatchKey')),
                         Column('KlassementKey', Integer, ForeignKey('Dim_Klassement.KlassementKey')),
                         Column('DateKey', Integer, ForeignKey('Dim_Date.DateKey')),
                         Column('TimeKey', Integer, ForeignKey('Dim_Time.TimeKey')),
                         Column('OddsThuisWint', Float),
                         Column('OddsUitWint', Float),
                         Column('OddsBeideTeamsScoren', Float),
                         Column('OddsNietBeideTeamsScoren', Float),
                         Column('OddsMeerDan2_5', Float),
                         Column('OddsMinderDan2_5', Float),
                         )

# Creëer alle tabellen in de database
metadata.create_all(engine)

# Functie om de vaste waarden in Dim_Kansen in te voegen
def insert_dim_kansen_values():
    session = Session()
    try:
        # Check of waarden al bestaan om duplicaten te voorkomen
        existing_values = session.query(Dim_Kansen.c.Waarde).all()
        existing_values = [value[0] for value in existing_values]
        
        # Te inserten waarden
        kansen_values = [1.5, 2.5, 3.5]
        for value in kansen_values:
            if value not in existing_values:
                insert_stmt = insert(Dim_Kansen).values(Waarde=value)
                session.execute(insert_stmt)
                
        session.commit()
        logging.info("Vaste waarden succesvol ingevoegd in Dim_Kansen")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {e}")
    finally:
        session.close()

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
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data_stamnummer\stand_stamnummer.csv'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data_stamnummer\matches_stamnummer.csv',),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data_stamnummer\stand_stamnummer.csv'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\bet777\data\bets.csv'),
]

# Voeg de vaste waarden toe aan Dim_Kansen
insert_dim_kansen_values()

# Laad data in vanuit de opgegeven CSV bestanden naar de overeenstemmende tabellen
# load_data_from_csv_list(csv_file_paths)