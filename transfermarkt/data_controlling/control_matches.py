import pandas as pd
from sklearn.ensemble import IsolationForest

# Bestand inlezen
matches_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\clean_matches.csv')

# Controle op ontbrekende waarden
missing_values = matches_df.isnull().sum()
print("Ontbrekende waarden:")
print(missing_values)

#Ongeldige Resultaten
invalid_results = matches_df[(matches_df['Resultaat_Thuisploeg'] < 0) | (matches_df['Resultaat_Uitploeg'] < 0)]
if not invalid_results.empty:
    print("\nOngeldige resultaten gevonden:")
    print(invalid_results[['Match_ID', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])

# Controle op unieke identificatie (Match_ID)
duplicate_match_ids = matches_df[matches_df.duplicated(subset=['Match_ID'], keep=False)]
if not duplicate_match_ids.empty:
    print("\nDuplicaat Match_ID's gevonden:")
    print(duplicate_match_ids[['Match_ID']])

# Controle op consistentie van datatypes
data_types = matches_df.dtypes
print("\nDatatypes:")
print(data_types)

# Controle op outlier detectie met Isolation Forest
outlier_detector = IsolationForest(contamination=0.5)  # Controle op 10% van de data als outlier
outlier_detector.fit(matches_df[['Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])

outliers = outlier_detector.predict(matches_df[['Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])
matches_df['Outlier'] = outliers

# Selecteer de rijen die als outliers zijn geclassificeerd
outliers_resultaten = matches_df[matches_df['Outlier'] == -1]
if not outliers_resultaten.empty:
    print("\nOutliers in resultaten gevonden:")
    print(outliers_resultaten[['Match_ID', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])

# Controle op consistentie tussen gerelateerde velden
inconsistent_teams = matches_df[(matches_df['Thuisploeg_stamnummer'] == 0) & (matches_df['Thuisploeg'] != 'Onbekend')]
inconsistent_teams = inconsistent_teams._append(matches_df[(matches_df['Uitploeg_stamnummer'] == 0) & (matches_df['Uitploeg'] != 'Onbekend')], ignore_index=True)
if not inconsistent_teams.empty:
    print("\nInconsistentie in teams gevonden:")
    print(inconsistent_teams[['Match_ID', 'Thuisploeg', 'Thuisploeg_stamnummer', 'Uitploeg', 'Uitploeg_stamnummer']])

# Uitvoer weergeven
print("\nMatch DataFrame:")
print(matches_df)


# groepeer de gegevens op seizoen, speeldag en ploeg
grouped_data_thuis = matches_df.groupby(['Seizoen', 'Speeldag', 'Match_ID'])
grouped_data_uit = matches_df.groupby(['Seizoen', 'Speeldag', 'Match_ID'])

# bereken het totaal aantal gewonnen wedstrijden per seizoen en per ploeg voor thuis- en uitploeg
totaal_gewonnen_thuis = grouped_data_thuis['Resultaat_Thuisploeg'].sum()
totaal_gewonnen_uit = grouped_data_uit['Resultaat_Uitploeg'].sum()

# Zet de resultaten om in DataFrames en reset de index voor een betere weergave
totaal_gewonnen_thuis_df = totaal_gewonnen_thuis.reset_index()
totaal_gewonnen_uit_df = totaal_gewonnen_uit.reset_index()

if 'Verschil' not in matches_df.columns:
    matches_df['Verschil'] = matches_df['Resultaat_Thuisploeg'] - matches_df['Resultaat_Uitploeg']

# Check Resultaten of juist zijn en =! verschil
controle_aantal_wedstrijden = matches_df[(matches_df['Resultaat_Thuisploeg'] - matches_df['Resultaat_Uitploeg']) != matches_df['Verschil']]
if not controle_aantal_wedstrijden.empty:
    print("\nRecords gevonden waar (Thuisploeg - Uitploeg) niet gelijk is aan Verschil:")
    print(controle_aantal_wedstrijden)

# Voeg de resultaten voor thuis- en uitploeg samen
merged_results = pd.merge(totaal_gewonnen_thuis_df, totaal_gewonnen_uit_df, on=['Seizoen', 'Speeldag'])

# Hernoem de kolommen
merged_results.columns = ['Seizoen', 'Speeldag', 'Thuisploeg', 'Resultaat_Thuisploeg', 'Uitploeg', 'Resultaat_Uitploeg']

# Opslaan van de gecontroleerde gegevens in matches_controlled.csv
merged_results.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\gesorteerde_matches.csv', index=False)
# Opslaan van gecontroleerde resultaten
controlled_matches_filename = r'C:\Users\ayman\OneDrive\Bureaublad\Backup\matches_controlled.csv'
matches_df.to_csv(controlled_matches_filename, index=False)
print(f"\nGecontroleerde resultaten opgeslagen in {controlled_matches_filename}")
