import pandas as pd
from tqdm.auto import tqdm

def control_data(file_path, clean_matches_file_path):
    # Load the CSV file
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')

    jaarTallen2puntensysteem = set(range(1960, 1995)) - {1964}
    
    # Initialize a counter for errors
    error_count = 0

    # Load the voetbalkrant CSV file
    try:
        data2 = pd.read_csv(clean_matches_file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data2 = pd.read_csv(clean_matches_file_path, encoding='ISO-8859-1')

    # Iterate over each row in DataFrame
    for index, row in tqdm(data.iterrows(), total=data.shape[0], desc="Verwerking van gegevens"):
        # Check doelpuntensaldo
        if (row['DoelpuntenVoor'] - row['DoelpuntenTegen']) != row['Doelpuntensaldo']:
            print(f"Fout in rij {index + 2}: Doelpuntensaldo klopt niet.")
            error_count += 1
        
        # Determine if season uses 2-point or 3-point system
        if row['SeizoensBegin'] in jaarTallen2puntensysteem:
            points_for_win = 2
        else:
            points_for_win = 3

        # Initialize expected points
        expected_points = row['AantalGewonnen'] * points_for_win + row['AantalGelijk']
        
        # Check if expected points match the actual points
        if expected_points != row['PuntenVoor']:
            print(f"Fout in rij {index + 2}: PuntenVoor klopt niet met verwachte punten.")
            error_count += 1

        # Additional checks based on match results
        matches = data2[(data2['Thuisploeg_stamnummer'] == row['Stamnummer']) | (data2['Uitploeg_stamnummer'] == row['Stamnummer'])]
        for _, match in matches.iterrows():
            seizoen = f"{row['SeizoensBegin']}-{row['SeizoensEinde']}"
            if match['Seizoen'] == seizoen and match['Speeldag'] == row['Speeldag']:
                continue
                
    
    if error_count == 0:
        print("Alle checks zijn succesvol. Geen fouten gevonden.")
    else:
        print(f"Aantal fouten gevonden: {error_count}")

    return data


# Path to your cleaned CSV file
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
clean_matches_file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\matches_clean.csv'

controlled_data = control_data(file_path, clean_matches_file_path)

# Save the controlled data to a new CSV
controlled_data.to_csv(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\controlled_data\stand_controlled.csv', index=False)