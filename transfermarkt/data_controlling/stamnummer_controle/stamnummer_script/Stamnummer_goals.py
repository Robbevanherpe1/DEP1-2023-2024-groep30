import pandas as pd

# CSV-bestanden laden
stamnummer_df = pd.read_csv(r'DEP-G30\transfermarkt\data_controlling\stamnummer_controle\stamnummers_data\stamnummer.csv')
goals_df = pd.read_csv(r'DEP-G30\transfermarkt\data\cleaned_data\goals_clean.csv')
matches_df = pd.read_csv(r'DEP-G30\transfermarkt\data\cleaned_data\matches_clean.csv')
stand_df = pd.read_csv(r'DEP-G30\transfermarkt\data\cleaned_data\stand_clean.csv')

# Een mapping creëren van teamnamen naar stamnummers
# Ervan uitgaande dat 'Thuisploeg' en 'Uitploeg' in stamnummer.csv identiek zijn en alle teamvarianten dekken
team_to_stamnummer = pd.Series(stamnummer_df.Stamnummer.values, index=stamnummer_df.Thuisploeg).to_dict()

# Het dataframe voor doelpunten bijwerken
goals_df['Thuisploeg_stamnummer'] = goals_df['Thuisploeg'].map(team_to_stamnummer).astype(pd.Int64Dtype())
goals_df['Uitploeg_stamnummer'] = goals_df['Uitploeg'].map(team_to_stamnummer).astype(pd.Int64Dtype())

# Het dataframe voor wedstrijden bijwerken
matches_df['Thuisploeg_stamnummer'] = matches_df['Thuisploeg'].map(team_to_stamnummer).astype(pd.Int64Dtype())
matches_df['Uitploeg_stamnummer'] = matches_df['Uitploeg'].map(team_to_stamnummer).astype(pd.Int64Dtype())

# Het stand_dataframe bijwerken
# Ervan uitgaande dat 'Club' in stand_clean.csv overeenkomt met 'Thuisploeg'/'Uitploeg' in stamnummer.csv
stand_df['Ploeg_stamnummer'] = stand_df['Club'].map(team_to_stamnummer).astype(pd.Int64Dtype())

# De bijgewerkte dataframes opslaan
goals_df.to_csv(r'DEP-G30\transfermarkt\data\controlled_data\controlled_data_stamnummer\goals_stamnummer.csv', index=False)
matches_df.to_csv(r'DEP-G30\transfermarkt\data\controlled_data\controlled_data_stamnummer\matches_stamnummer.csv', index=False)
stand_df.to_csv(r'DEP-G30\transfermarkt\data\controlled_data\controlled_data_stamnummer\stand_stamnummer.csv', index=False)
