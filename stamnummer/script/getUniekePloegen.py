import pandas as pd

# Lees de originele CSV in
originele_csv = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'

df = pd.read_csv(originele_csv)

# Extract de kolom met de clubs/ploegen
clubs = df['Club']

# Stap 3: Vind alle unieke waarden
unieke_clubs = clubs.unique()

# Stap 4: Schrijf de unieke waarden naar een nieuw CSV-bestand
nieuw_csv = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\stamnummer\data\unieke_ploegen.csv'
pd.Series(unieke_clubs).to_csv(nieuw_csv, index=False, header=['Club'])

print(f'De lijst van unieke ploegen is opgeslagen in {nieuw_csv}.')
