import pandas as pd

def clean_data(file_path):
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')
    
    seizoen_split = data['Seizoen'].str.split('-', expand=True)
    data['SeizoensBegin'] = seizoen_split[0].astype(int)
    data['SeizoensEinde'] = seizoen_split[1].astype(int)
    
    punten_split = data['Punten'].str.split(':', expand=True)
    data['PuntenVoor'] = punten_split[0]
    data['PuntenTegen'] = punten_split[1]
    
    doelpunten_split = data['Doelpunten'].str.split(':', expand=True)
    data['DoelpuntenVoor'] = doelpunten_split[0]
    data['DoelpuntenTegen'] = doelpunten_split[1]
    
    # Correctly calculate 'PuntenTegen' for seasons after 1995-1996 using a vectorized approach
    condition = (data['SeizoensBegin'] >= 1995) | ((data['SeizoensBegin'] == 1964) & (data['SeizoensEinde'] == 1965))
    data.loc[condition, 'PuntenTegen'] = data['AantalGelijk'] * 1 + data['AantalVerloren'] * 3

    data.drop(['Seizoen', 'Punten', 'Doelpunten'], axis=1, inplace=True)
    
    columns_before = ['SeizoensBegin', 'SeizoensEinde', 'Speeldag', 'Stand', 'Club', 'AantalGespeeld', 'AantalGewonnen', 'AantalGelijk', 'AantalVerloren']
    columns_after = ['Doelpuntensaldo', 'PuntenVoor', 'PuntenTegen']
    data = data[columns_before + ['DoelpuntenVoor', 'DoelpuntenTegen'] + columns_after]
    
    return data


# Replace this with your actual file path
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\scraped_data\stand.csv'
cleaned_data = clean_data(file_path)

# Save the cleaned data to a new CSV
cleaned_data.to_csv('stand_clean.csv', index=False)
