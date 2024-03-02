import pandas as pd

def clean_data(file_path):
    # Attempt to load the CSV file with UTF-8 encoding
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        # If a UnicodeDecodeError occurs, try loading with ISO-8859-1 encoding
        data = pd.read_csv(file_path, encoding='ISO-8859-1')
    
    # Split 'Seizoen' into 'SeizoensBegin' and 'SeizoensEinde'
    seizoen_split = data['Seizoen'].str.split('-', expand=True)
    data['SeizoensBegin'] = seizoen_split[0]
    data['SeizoensEinde'] = seizoen_split[1]
    
    # Split 'Punten' into 'PuntenVoor' and 'PuntenTegen'
    punten_split = data['Punten'].str.split(':', expand=True)
    data['PuntenVoor'] = punten_split[0]
    data['PuntenTegen'] = punten_split[1]

    # Split 'Doelpunten' into 'DoelpuntenVoor' and 'DoelpuntenTegen'
    doelpunten_split = data['Doelpunten'].str.split(':', expand=True)
    data['DoelpuntenVoor'] = doelpunten_split[0]
    data['DoelpuntenTegen'] = doelpunten_split[1]
    
    # Calculate 'PuntenTegen' for seasons after 1995-1996
    for i, row in data.iterrows():
        seizoens_einde = int(row['SeizoensEinde'])
        if seizoens_einde > 1996:
            data.at[i, 'PuntenTegen'] = row['AantalGelijk'] * 1 + row['AantalVerloren'] * 3

    # Remove the original 'Seizoen', 'Punten', and 'Doelpunten' columns
    data.drop(['Seizoen', 'Punten', 'Doelpunten'], axis=1, inplace=True)
    
    # Reorder columns to place 'DoelpuntenVoor' and 'DoelpuntenTegen' right before 'Doelpuntensaldo'
    columns_before = ['SeizoensBegin', 'SeizoensEinde', 'Speeldag', 'Stand', 'Club', 'AantalGespeeld', 'AantalGewonnen', 'AantalGelijk', 'AantalVerloren']
    columns_after = ['Doelpuntensaldo', 'PuntenVoor', 'PuntenTegen']
    data = data[columns_before + ['DoelpuntenVoor', 'DoelpuntenTegen'] + columns_after]
    
    return data

# Replace this with your actual file path
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\scraped_data\stand.csv'
cleaned_data = clean_data(file_path)

# Save the cleaned data to a new CSV
cleaned_data.to_csv('stand_clean.csv', index=False)
