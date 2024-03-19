import pandas as pd

# Bestanden inlezen
wedstrijden_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\clean_matches.csv', sep=',' )
doelpunten_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\clean_goals.csv' , sep=';')
stamnummer_df = pd.read_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\stamnummers.csv', sep=';')
print(wedstrijden_df.columns)
print(wedstrijden_df.columns)


# Datum- en tijdstipformaten corrigeren en samenvoegen tot één datetime kolom
wedstrijden_df['DateTime'] = pd.to_datetime(wedstrijden_df['Datum'] + ' ' + wedstrijden_df['Tijdstip'])
doelpunten_df['GoalDateTime'] = pd.to_datetime(doelpunten_df['Datum'] + ' ' + doelpunten_df['GoalTijdstip'], errors='coerce')

# Controleer doelpunttijdstippen binnen 4 uur na start van de match
doelpunten_df = doelpunten_df.merge(wedstrijden_df[['Match_ID', 'DateTime']], on='Match_ID', how='left')
doelpunten_df['TimeDelta'] = (doelpunten_df['GoalDateTime'] - doelpunten_df['DateTime']).dt.total_seconds() / 3600
invalid_goals = doelpunten_df[(doelpunten_df['TimeDelta'] < 0) | (doelpunten_df['TimeDelta'] > 4)]

# Controleer overeenkomst van datums tussen wedstrijden en doelpunten
merged_df = doelpunten_df.merge(wedstrijden_df[['Match_ID', 'Datum']], on='Match_ID', suffixes=('_Goal', '_Match'))
date_mismatches = merged_df[merged_df['Datum_Goal'] != merged_df['Datum_Match']]

# Resultaten opslaan

# Ongeldige doelpunten zonder ongewenste kolommen
invalid_goals_output = invalid_goals.drop(columns=['GoalDateTime', 'DateTime', 'TimeDelta'])
invalid_goals_output.to_csv('invalid_goals.csv', index=False)

# Datum mismatches zonder ongewenste kolommen
date_mismatches_output = date_mismatches.drop(columns=['GoalDateTime', 'DateTime', 'TimeDelta'])
date_mismatches_output.to_csv('date_mismatches_goals.csv', index=False)

merged_thuisploeg = pd.merge(doelpunten_df, stamnummer_df, left_on='Thuisploeg_stamnummer', right_on='stamnummer', how='left')

merged_uitploeg = pd.merge(merged_thuisploeg, stamnummer_df, left_on='Uitploeg_stamnummer', right_on='stamnummer', how='left', suffixes=('_thuis', '_uit'))

merged_uitploeg['Thuisploeg'] = merged_uitploeg['club_naam_thuis']
merged_uitploeg['Uitploeg'] = merged_uitploeg['club_naam_uit']


merged_uitploeg.drop(['club_naam_thuis', 'club_naam_uit', 'stamnummer_thuis', 'stamnummer_uit'], axis=1, inplace=True)

# Check for valid matches
# Ensure both Thuisploeg and Uitploeg match their respective stamnummers
valid_matches = merged_uitploeg[(merged_uitploeg['Thuisploeg'] == merged_uitploeg['roepnaam_thuis']) & (merged_uitploeg['Uitploeg'] == merged_uitploeg['roepnaam_uit'])]
invalid_matches = merged_uitploeg[(merged_uitploeg['Thuisploeg'] != merged_uitploeg['roepnaam_thuis']) | (merged_uitploeg['Uitploeg'] != merged_uitploeg['roepnaam_uit'])]

for index, row in invalid_matches.iterrows():
    if row['Thuisploeg'] != row['roepnaam_thuis']:
        doelpunten_df.loc[doelpunten_df['Match_ID'] == row['Match_ID'], 'Thuisploeg'] = row['roepnaam_thuis']
    if row['Uitploeg'] != row['roepnaam_uit']:
        doelpunten_df.loc[doelpunten_df['Match_ID'] == row['Match_ID'], 'Uitploeg'] = row['roepnaam_uit']

doelpunten_df.to_csv('valid_matches.csv', index=False)