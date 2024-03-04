import pandas as pd

# Laad het huidige dataset
df_clubs = pd.read_csv(r'C:\Users\Robbe\Documents\School_jaar_2\sem2\DEP\development\cleaning\goals.csv')

# Laad het dataset met stamnummers
df_stamnummers = pd.read_csv(r'C:\Users\Robbe\Documents\School_jaar_2\sem2\DEP\development\cleaning\football_clubs_stamnummer.csv')

# Bereid df_stamnummers voor op samenvoegen door twee aparte DataFrames te maken voor thuisploeg en uitploeg
df_stamnummers_thuis = df_stamnummers[['Stamnummer', 'Thuisploeg']].rename(columns={'Thuisploeg': 'Club', 'Stamnummer': 'Stamnummer_Thuis'})
df_stamnummers_uit = df_stamnummers[['Stamnummer', 'Uitploeg']].rename(columns={'Uitploeg': 'Club', 'Stamnummer': 'Stamnummer_Uit'})

# Voeg df_clubs samen met df_stamnummers_thuis op "Thuisploeg"
df_clubs = pd.merge(df_clubs, df_stamnummers_thuis, left_on='Thuisploeg', right_on='Club', how='left').drop('Club', axis=1)

# Voeg df_clubs samen met df_stamnummers_uit op "Uitploeg"
df_clubs = pd.merge(df_clubs, df_stamnummers_uit, left_on='Uitploeg', right_on='Club', how='left').drop('Club', axis=1)

# Sla de samengevoegde dataframe op naar een nieuw CSV-bestand
df_clubs.to_csv(r'C:\Users\Robbe\Documents\School_jaar_2\sem2\DEP\development\cleaning\merged_goals_stamnummers.csv', index=False)
