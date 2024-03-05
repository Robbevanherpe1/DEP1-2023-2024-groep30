import pandas as pd
from sqlalchemy import Date, DateTime, Float, Time, create_engine, Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.exc import SQLAlchemyError

# Database connectie informatie
db_user = 'root'
db_password = 'root'
db_host = 'localhost'
db_port = '3306'
db_name = 'DEP_G30_DWH'

# MySQL connectie string
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Aanmaken van een MetaData instantie
metadata = MetaData()

# Tabellen aanmaken
Fact_JPL = Table('Fact_JPL', metadata,
                  Column('JPLKey', Integer, primary_key=True),
                  Column('StandKey', Integer, ForeignKey('Dim_Stand.StandKey')),
                  Column('GoalKey', Integer, ForeignKey('Dim_Goal.GoalKey')),
                  Column('WeddenschapKey', Integer, ForeignKey('Dim_Weddenschap.WeddenschapKey')),
                  Column('MatchID', String(50)),
                  Column('Seizoen', String(50)),
                  Column('Speeldag', String(50)),
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
                  Column('StandUitploeg', Integer)
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
                    Column('Uitploeg', String(255))
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
                        Column('Kans', Float)
                        )

# Creëer alle tabellen in de database
metadata.create_all(engine)

# Verbeterde functie om data te laden vanuit een lijst van CSV bestanden naar overeenstemmende tabellen
def load_data_from_csv_list(csv_file_paths):
  for file_path, table_name in csv_file_paths:
    try:
      df = pd.read_csv(file_path)
      df.to_sql(table_name, con=engine, if_exists='append', index=False)
      print(f"Data from {file_path} successfully loaded into {table_name}")
    except IOError:
      print(f"Error: File {file_path} does not exist or could not be read.")
    except SQLAlchemyError as e:
      print(f"Error loading data into {table_name}: {str(e)}")

# Lijst van CSV bestanden en hun overeenstemmende tabelnamen
csv_file_paths = [
    (r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv', 'Dim_Stand'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\matches_clean.csv', 'Dim_Match'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\goals_clean.csv', 'Dim_Goal'),
    (r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\bet777\data\bets.csv', 'Dim_Weddenschap'),
]

# Laad data in vanuit de opgegeven CSV bestanden naar de overeenstemmende tabellen
load_data_from_csv_list(csv_file_paths)