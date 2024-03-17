import pandas as pd

# Bestanden inlezen
wedstrijden_df = pd.read_csv('matches_clean2.csv')
doelpunten_df = pd.read_csv('goals_clean3.csv')

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