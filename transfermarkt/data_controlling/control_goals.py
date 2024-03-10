import pandas as pd

# Bestanden inlezen
wedstrijden_df = pd.read_csv('wedstrijden.csv')
doelpunten_df = pd.read_csv('doelpunten.csv')

# Datum- en tijdstipformaten corrigeren en samenvoegen tot één datetime kolom
wedstrijden_df['DateTime'] = pd.to_datetime(wedstrijden_df['Datum'] + ' ' + wedstrijden_df['Tijdstip'])
doelpunten_df['GoalDateTime'] = pd.to_datetime(doelpunten_df['Datum'] + ' ' + doelpunten_df['GoalTijdstip'], errors='coerce')

# Controleer doelpunttijdstippen binnen 4 uur na start van de match
doelpunten_df = doelpunten_df.merge(wedstrijden_df[['Match_ID', 'DateTime']], on='Match_ID', how='left')
doelpunten_df['TimeDelta'] = (doelpunten_df['GoalDateTime'] - doelpunten_df['DateTime']).dt.total_seconds() / 3600
valid_goals = doelpunten_df[(doelpunten_df['TimeDelta'] >= 0) & (doelpunten_df['TimeDelta'] <= 4)]
invalid_goals = doelpunten_df[(doelpunten_df['TimeDelta'] < 0) | (doelpunten_df['TimeDelta'] > 4)]

# Controleer overeenkomst van datums tussen wedstrijden en doelpunten
doelpunten_df['DateMatch'] = doelpunten_df['DateTime'].dt.date
doelpunten_df['DateGoal'] = doelpunten_df['GoalDateTime'].dt.date
date_mismatches = doelpunten_df[doelpunten_df['DateMatch'] != doelpunten_df['DateGoal']]


# Resultaten opslaan

# Geldige doelpunten zonder ongewenste kolommen
valid_goals_output = valid_goals.drop(columns=['GoalDateTime', 'DateTime', 'TimeDelta'])
valid_goals_output.to_csv('valid_goals.csv', index=False)

# Ongeldige doelpunten zonder ongewenste kolommen
invalid_goals_output = invalid_goals.drop(columns=['GoalDateTime', 'DateTime', 'TimeDelta'])
invalid_goals_output.to_csv('invalid_goals.csv', index=False)

# Datum mismatches zonder ongewenste kolommen
date_mismatches_output = date_mismatches.drop(columns=['GoalDateTime', 'DateTime', 'TimeDelta'])
date_mismatches_output.to_csv('date_mismatches_goals.csv', index=False)