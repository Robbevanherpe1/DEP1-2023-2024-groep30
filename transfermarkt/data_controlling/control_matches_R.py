import pandas as pd
from sklearn.ensemble import IsolationForest

# Laad de dataset
matches_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\clean_matches.csv')

# Controleer op ontbrekende waarden en ongeldige resultaten
print("Ontbrekende waarden:", matches_df.isnull().sum())
ongeldige_resultaten = matches_df[(matches_df['Resultaat_Thuisploeg'] < 0) | (matches_df['Resultaat_Uitploeg'] < 0)]
if not ongeldige_resultaten.empty:
    print("\nOngeldige resultaten gevonden:", ongeldige_resultaten[['Match_ID', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])

# Controleer op dubbele Match_ID's
dubbele_match_ids = matches_df[matches_df.duplicated(subset=['Match_ID'], keep=False)]
if not dubbele_match_ids.empty:
    print("\nDuplicaat Match_ID's gevonden:", dubbele_match_ids[['Match_ID']])

# Controleer datatypes
print("\nDatatypes:", matches_df.dtypes)

# Outlier detectie met Isolation Forest
outlier_detector = IsolationForest(contamination=0.5)
outlier_detector.fit(matches_df[['Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])
matches_df['Outlier'] = outlier_detector.predict(matches_df[['Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])
outliers_resultaten = matches_df[matches_df['Outlier'] == -1]
if not outliers_resultaten.empty:
    print("\nOutliers in resultaten gevonden:", outliers_resultaten[['Match_ID', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])

# Controleer consistentie tussen gerelateerde velden
inconsistent_teams = matches_df[(matches_df['Thuisploeg_stamnummer'] == 0) & (matches_df['Thuisploeg'] != 'Onbekend') | (matches_df['Uitploeg_stamnummer'] == 0) & (matches_df['Uitploeg'] != 'Onbekend')]
if not inconsistent_teams.empty:
    print("\nInconsistentie in teams gevonden:", inconsistent_teams[['Match_ID', 'Thuisploeg', 'Thuisploeg_stamnummer', 'Uitploeg', 'Uitploeg_stamnummer']])

# Toon de DataFrame
print("\nMatch DataFrame:", matches_df)

# Groepeer gegevens en bereken totale overwinningen
gegroepeerde_data = matches_df.groupby(['Seizoen', 'Speeldag', 'Match_ID'])
totaal_gewonnen_thuis = gegroepeerde_data['Resultaat_Thuisploeg'].sum().reset_index()
totaal_gewonnen_uit = gegroepeerde_data['Resultaat_Uitploeg'].sum().reset_index()

# Controleer of 'Verschil' kolom bestaat en bereken indien niet
if 'Verschil' not in matches_df.columns:
    matches_df['Verschil'] = matches_df['Resultaat_Thuisploeg'] - matches_df['Resultaat_Uitploeg']

# Controleer of het verschil tussen thuis- en uitploegresultaten gelijk is aan de 'Verschil' kolom
controle_aantal_wedstrijden = matches_df[(matches_df['Resultaat_Thuisploeg'] - matches_df['Resultaat_Uitploeg']) != matches_df['Verschil']]
if not controle_aantal_wedstrijden.empty:
    print("\nRecords gevonden waar (Thuisploeg - Uitploeg) niet gelijk is aan Verschil:", controle_aantal_wedstrijden)

# Voeg resultaten samen en hernoem kolommen
samengevoegde_resultaten = pd.merge(totaal_gewonnen_thuis, totaal_gewonnen_uit, on=['Seizoen', 'Speeldag'])
samengevoegde_resultaten.columns = ['Seizoen', 'Speeldag', 'Thuisploeg', 'Resultaat_Thuisploeg', 'Uitploeg', 'Resultaat_Uitploeg']

# Bewaar de gecontroleerde gegevens
samengevoegde_resultaten.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\gesorteerde_matches.csv', index=False)
matches_df.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\matches_controlled.csv', index=False)
print(f"\nGecontroleerde resultaten opgeslagen in {r'C:\Users\ayman\OneDrive\Bureaublad\Backup\matches_controlled.csv'}")


# goals_df geladen is van doelpunten.csv
goals_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\doelpunten.csv')

# Controleer op 0-0 wedstrijden en verifieer geen doelpunten geregistreerd
zero_zero_matches = matches_df[(matches_df['Resultaat_Thuisploeg'] == 0) & (matches_df['Resultaat_Uitploeg'] == 0)]
if not zero_zero_matches.empty:
    print("\nMatchen die eindigden op 0-0:", zero_zero_matches[['Match_ID']])
    for match_id in zero_zero_matches['Match_ID']:
        if not goals_df[goals_df['Match_ID'] == match_id].empty:
            print(f"Fout: Match {match_id} eindigde op 0-0 maar er zijn doelpunten geregistreerd.")

# Aannemend dat standings_df geladen is van je standings data
standings_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\standings.csv')

# Bereken standings van match resultaten
standings = matches_df.groupby(['Team']).agg({
    'Match_ID': 'count',
    'Resultaat_Thuisploeg': 'sum',
    'Resultaat_Uitploeg': 'sum'
}).rename(columns={
    'Match_ID': 'Aantal wedstrijden',
    'Resultaat_Thuisploeg': 'Aantal gewonnen wedstrijden',
    'Resultaat_Uitploeg': 'Aantal verloren wedstrijden'
})

# Aanvullende berekeningen voor standings
standings['Aantal gelijkgespeelde wedstrijden'] = matches_df.groupby('Team').apply(lambda x: (x['Resultaat_Thuisploeg'] == x['Resultaat_Uitploeg']).sum())
standings['Doelpunten'] = matches_df.groupby('Team').apply(lambda x: x['Resultaat_Thuisploeg'].sum() + x['Resultaat_Uitploeg'].sum())
standings['+/-'] = standings['Doelpunten'] - (standings['Aantal gewonnen wedstrijden'] - standings['Aantal verloren wedstrijden'])

# Vergelijk berekende standings met bestaande standings
if not standings.equals(standings_df):
    print("Fout: De berekende standings komen niet overeen met de bestaande standings.")

# Pas standings berekening aan op basis van scoringsysteem
standings['Punten'] = standings.apply(lambda row: row['Aantal gewonnen wedstrijden'] * 3 if row['Seizoen'] >= 1995 else row['Aantal gewonnen wedstrijden'] * 2, axis=1)

# Vergelijk aangepaste standings met bestaande standings
if not standings[['Punten']].equals(standings_df[['Punten']]):
    print("Fout: De berekende punten komen niet overeen met de bestaande punten.")
    
# check correct and incorrecte matchen
correct_matches = matches_df[matches_df['Outlier'] != -1]
incorrect_matches = matches_df[matches_df['Outlier'] == -1]

# Save correcte matchen naar CSV
correct_matches.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\correct_matches.csv', index=False)

# Save incorrecte matchen naar CSV
incorrect_matches.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\incorrect_matches.csv', index=False)

# Save alle matchen naar CSV
matches_df.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\all_matches.csv', index=False)

print("Correcte matchen saved in correct_matches.csv")
print("Incorrecte matchen saved in incorrect_matches.csv")
print("Alle matchen saved in all_matches.csv")
