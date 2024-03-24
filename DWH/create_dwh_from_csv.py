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
Dim_Kans = Table('Dim_Kans', metadata,
                    Column('KansKey', Integer, primary_key=True, autoincrement=True),
                    Column('OddsWaarde', Float, unique=True),
                    )

Dim_Team = Table('Dim_Team', metadata,
                    Column('Teamkey', Integer, primary_key=True),
                    Column('Id', String(255)),
                    Column('Stamnummer', String(255)),
                    Column('PloegNaam', String(255)),
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

Dim_Wedstrijd = Table('Dim_Wedstrijd', metadata,
                    Column('WedstrijdKey', Integer, primary_key=True),
                    Column('MatchID', Integer),
                    Column('Uitploeg', String(255)),
                    Column('Thuisploeg', String(255)),
                    )

# Fact tabellen
Fact_WedstrijdScore = Table('Fact_WedstrijdScore', metadata,
                            Column('WedstrijdScoreID', Integer, primary_key=True, autoincrement=True),
                            Column('Teamkey', Integer, ForeignKey('Dim_Team.Teamkey')),
                            Column('WedstrijdKey', Integer, ForeignKey('Dim_Wedstrijd.WedstrijdKey')),
                            Column('KlassementKey', Integer, ForeignKey('Dim_Klassement.KlassementKey')),
                            Column('KansKey', Integer, ForeignKey('Dim_Kans.KansKey')),
                            Column('DateKey', Integer, ForeignKey('Dim_Date.DateKey')),
                            Column('TimeKey', Integer, ForeignKey('Dim_Time.TimeKey')),
                            Column('ScoreThuis', Integer),
                            Column('ScoreUit', Integer),
                            Column('EindscoreThuis', Integer),
                            Column('EindscoreUit', Integer),
                            Column('ScorendePloegIndicator', String(255)),
                            Column('DoelpuntenMaker', String(255)),
                            )

Fact_Weddenschap = Table('Fact_Weddenschap', metadata,
                            Column('WeddenschapID', Integer, primary_key=True, autoincrement=True),
                            Column('Teamkey', Integer, ForeignKey('Dim_Team.Teamkey')),
                            Column('WedstrijdKey', Integer, ForeignKey('Dim_Wedstrijd.WedstrijdKey')),
                            Column('KlassementKey', Integer, ForeignKey('Dim_Klassement.KlassementKey')),
                            Column('KansKey', Integer, ForeignKey('Dim_Kans.KansKey')),
                            Column('DateKey', Integer, ForeignKey('Dim_Date.DateKey')),
                            Column('TimeKey', Integer, ForeignKey('Dim_Time.TimeKey')),
                            Column('OddsThuisWint', Float),
                            Column('OddsUitWint', Float),
                            Column('OddsBeideTeamsScoren', Float),
                            Column('OddsNietBeideTeamsScoren', Float),
                            Column('OddsMeerDanX', Float),
                            Column('OddsMinderDanX', Float),
                            )

# Creëer alle tabellen in de database
metadata.create_all(engine)

# Functie om de vaste OddsWaarde in Dim_Kansen in te voegen
def insert_dim_kansen_values():
    session = Session()
    try:
        # Check of OddsWaarde al bestaan om duplicaten te voorkomen
        existing_values = session.query(Dim_Kans.c.OddsWaarde).all()
        existing_values = [value[0] for value in existing_values]
        
        # Te inserten waarden
        kansen_values = [1.5, 2.5, 3.5]
        for value in kansen_values:
            if value not in existing_values:
                insert_stmt = insert(Dim_Kans).values(OddsWaarde=value)
                session.execute(insert_stmt)
                
        session.commit()
        logging.info("Vaste waarden succesvol ingevoegd in Dim_Kans")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {e}")
    finally:
        session.close()

# Lijst van CSV bestanden en hun overeenstemmende tabelnamen
csv_file_paths = [
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\goals_controlled.csv'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\matches_controlled.csv'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\bet777\data\bets.csv'),
]

# Voeg de vaste OddsWaarden toe aan Dim_Kansen
insert_dim_kansen_values()

# Laad data in vanuit de opgegeven CSV bestanden naar de overeenstemmende tabellen
# load_data_from_csv_list(csv_file_paths) TO DO 

def load_data_from_csv_list(csv_file_paths):
    session = Session()
    try:
        for file_path in csv_file_paths:
            # Lees het CSV-bestand met Pandas
            df = pd.read_csv(file_path)
            
            # Bepaal de tabelnaam op basis van het bestandspath
            table_name = os.path.basename(file_path).split('.')[0] # Haal de bestandsnaam zonder extensie
            table = Table(table_name, metadata, autoload_with=engine)
            
            # Converteer de DataFrame naar een lijst van dictionaries
            records = df.to_dict(orient='records')
            
            # Voeg de records toe aan de database met bulk insert
            session.bulk_insert_mappings(table, records)
        
        session.commit()
        logging.info("Gegevens succesvol geladen in de database")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {e}")
    finally:
        session.close()