import pandas as pd
from sklearn.ensemble import IsolationForest
import pandas as pd

#Laad de dataset
matches_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\clean_matches.csv', sep=',', header=0)
stamnummer_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\stamnummers.csv', sep=';', header=0)
# Checkt voor de juiste data types
expected_dtypes = {
    'Seizoen': 'int64',
    'Speeldag': 'int64',
    'Match_ID': 'int64',
    'Thuisploeg': 'object',
    'Uitploeg': 'object',
    'Thuisploeg_stamnummer': 'int64',
    'Uitploeg_stamnummer': 'int64',
    'Resultaat_Thuisploeg': 'int64',
    'Resultaat_Uitploeg': 'int64',
    'Outlier': 'int64'
}


column_dtypes_df = pd.DataFrame({'Column': matches_df.columns, 'DataType': matches_df.dtypes})


expected_dtypes_df = pd.DataFrame(list(expected_dtypes.items()), columns=['Column', 'ExpectedDataType'])


merged_df = pd.merge(column_dtypes_df, expected_dtypes_df, on='Column', how='left')


mismatched_dtypes = merged_df[merged_df['DataType'] != merged_df['ExpectedDataType']]

print("\nColumns with mismatched data types:")
print(mismatched_dtypes[['Column', 'DataType', 'ExpectedDataType']])

# Kijkt voor missende waarden
missing_values = matches_df.isnull().sum()
print("Missing values:")
print(missing_values)

# Ongeldige Speeldagen
invalid_matchdays = matches_df[matches_df['Speeldag'] < 1]
print("\nOngeldige speeldagen gevonden:")
print(invalid_matchdays[['Match_ID', 'Speeldag']])

#Ongeldige Seizoenen
invalid_seasons = matches_df[~matches_df['Seizoen'].astype(str).str.match(r'\d{4}-\d{4}')]
print("\nOngeldige seizoenen gevonden:")
print(invalid_seasons[['Match_ID', 'Seizoen']])

# Ongeldige datums
matches_df['Datum'] = pd.to_datetime(matches_df['Datum'])
invalid_dates = matches_df[matches_df['Datum'] > pd.Timestamp.now()]

print("\nMatches with invalid dates:")
print(invalid_dates[['Match_ID', 'Datum']])



# Kijkt naar het resultaat adhv de verschil kolom

matches_df['verschil'] = matches_df['Resultaat_Thuisploeg'] - matches_df['Resultaat_Uitploeg']
controle_aantal_wedstrijden = matches_df[(matches_df['Resultaat_Thuisploeg'] - matches_df['Resultaat_Uitploeg']) != matches_df['verschil']]

record_count = controle_aantal_wedstrijden.shape[0]


print(f"Number of records where the difference does not match 'verschil': {record_count}")

print("\nRecords where the difference does not match 'verschil':")
print(controle_aantal_wedstrijden)

# Kijk voor afwijkende resultaten
outlier_detector = IsolationForest(contamination=0.5)
outlier_detector.fit(matches_df[['Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])

outliers = outlier_detector.predict(matches_df[['Resultaat_Thuisploeg', 'Resultaat_Uitploeg']])
matches_df['Outlier'] = outliers

invalid_results = matches_df[matches_df['Outlier'] == -1]

# Kijkt voor duplicaten in de Match_ID kolom
duplicate_match_ids = matches_df[matches_df.duplicated(subset=['Match_ID'], keep=False)]

# Kijkt voor inconsistenties in de teams
inconsistent_teams = matches_df[(matches_df['Thuisploeg_stamnummer'] == 0) & (matches_df['Thuisploeg'] != 'Onbekend')]
inconsistent_teams = pd.concat([inconsistent_teams, matches_df[(matches_df['Uitploeg_stamnummer'] == 0) & (matches_df['Uitploeg'] != 'Onbekend')]], ignore_index=True)


# Merge matches_df with stamnummer_df on Thuisploeg_stamnummer
merged_thuisploeg = pd.merge(matches_df, stamnummer_df, left_on='Thuisploeg_stamnummer', right_on='stamnummer', how='left')

# Merge the result with stamnummer_df on Uitploeg_stamnummer
merged_uitploeg = pd.merge(merged_thuisploeg, stamnummer_df, left_on='Uitploeg_stamnummer', right_on='stamnummer', how='left', suffixes=('_thuis', '_uit'))

# Update the 'Thuisploeg' and 'Uitploeg' columns
merged_uitploeg['Thuisploeg'] = merged_uitploeg['club_naam_thuis']
merged_uitploeg['Uitploeg'] = merged_uitploeg['club_naam_uit']

# Drop unnecessary columns
merged_uitploeg.drop(['club_naam_thuis', 'club_naam_uit', 'stamnummer_thuis', 'stamnummer_uit'], axis=1, inplace=True)

# Check for valid matches
# Ensure both Thuisploeg and Uitploeg match their respective stamnummers
valid_matches = merged_uitploeg[(merged_uitploeg['Thuisploeg'] == merged_uitploeg['roepnaam_thuis']) & (merged_uitploeg['Uitploeg'] == merged_uitploeg['roepnaam_uit'])]
invalid_matches = merged_uitploeg[(merged_uitploeg['Thuisploeg'] != merged_uitploeg['roepnaam_thuis']) | (merged_uitploeg['Uitploeg'] != merged_uitploeg['roepnaam_uit'])]

for index, row in invalid_matches.iterrows():
    if row['Thuisploeg'] != row['roepnaam_thuis']:
        matches_df.loc[matches_df['Match_ID'] == row['Match_ID'], 'Thuisploeg'] = row['roepnaam_thuis']
    if row['Uitploeg'] != row['roepnaam_uit']:
        matches_df.loc[matches_df['Match_ID'] == row['Match_ID'], 'Uitploeg'] = row['roepnaam_uit']


# Drop unnecessary columns na de controle
matches_df.drop(['verschil', 'Outlier'], axis=1, inplace=True)
matches_df.drop(['Thuisploeg_roepnaam','Uitploeg_roepnaam' ], axis=1, inplace=True)
# Print the valid and invalid matches
matches_df.to_csv('valid_matches.csv', index=False)

# Save the filtered DataFrames to CSV files
invalid_results.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\invalid_results.csv', index=False)
duplicate_match_ids.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\duplicate_match_ids.csv', index=False)
inconsistent_teams.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\inconsistent_teams.csv', index=False)

# For checks that require logical conditions, we'll keep the traditional approach
# For example, checking if a value is less than 0 or if a column's dtype is not 'int64'
# These checks cannot be entirely replaced with the filtering approach shown above