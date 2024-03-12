import pandas as pd
from sklearn.ensemble import IsolationForest

# Bestand inlezen
matches_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\clean_matches.csv')

# Controle op ontbrekende waarden
missing_values = matches_df.isnull().sum()
print("Ontbrekende waarden:")
print(missing_values)

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

# Opslaan van gecontroleerde resultaten
controlled_matches_filename = r'C:\Users\ayman\OneDrive\Bureaublad\Backup\matches_controlled.csv'
matches_df.to_csv(controlled_matches_filename, index=False)
print(f"\nGecontroleerde resultaten opgeslagen in {controlled_matches_filename}")
