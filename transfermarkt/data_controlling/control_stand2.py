import pandas as pd

# Load the data
file_path_cleaned_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
data = pd.read_csv(file_path_cleaned_data, encoding='utf-8')

# Correct points calculation, accounting for the different points system before and after 1995, excluding 1964
data['CorrectPoints'] = data.apply(
    lambda row: (row['AantalGewonnen'] * 2 if (1960 <= row['SeizoensBegin'] <= 1995) and (row['SeizoensBegin'] != 1964) else row['AantalGewonnen'] * 3) + (row['AantalGelijk']),
    axis=1
)

# Sort by season, then by corrected points, goal difference, and goals scored, resetting standings for each season
data_sorted = data.sort_values(
    by=['SeizoensBegin', 'SeizoensEinde', 'CorrectPoints', 'Doelpuntensaldo', 'DoelpuntenVoor'],
    ascending=[True, True, False, False, False]
)

data_sorted['FinalStandReset'] = data_sorted.groupby(['SeizoensBegin', 'SeizoensEinde']).cumcount() + 1

# Merge sorted data back to original to find mismatches in standings
data_with_standings = data.merge(data_sorted[['SeizoensBegin', 'SeizoensEinde', 'Club', 'FinalStandReset']], on=['SeizoensBegin', 'SeizoensEinde', 'Club'], how='left')

# Identify mismatches where original 'Stand' does not match 'FinalStandReset'
incorrect_standings = data_with_standings[data_with_standings['Stand'] != data_with_standings['FinalStandReset']]

# Drop duplicates, keeping only the first occurrence for each club within the same season
incorrect_standings_no_duplicates = incorrect_standings.drop_duplicates(subset=['SeizoensBegin', 'SeizoensEinde', 'Club'])

# Select relevant columns for the output
incorrect_standings_output_no_duplicates = incorrect_standings_no_duplicates[['SeizoensBegin', 'SeizoensEinde', 'Speeldag', 'Club', 'Stand', 'FinalStandReset']]

# Define the output path for the CSV file
output_path_incorrect_no_duplicates = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\data_errors\errors_stand.csv'

# Save to CSV
incorrect_standings_output_no_duplicates.to_csv(output_path_incorrect_no_duplicates, index=False)

print(f'Incorrect standings without duplicates saved to {output_path_incorrect_no_duplicates}')
