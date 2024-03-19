import pandas as pd
from fuzzywuzzy import process
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
def match_name(name, list_names, min_score=0):
    max_score = -1
    best_match = None
    for x in list_names:
        score = process.extractOne(name, [x], score_cutoff=min_score)
        if score:
            if score[1] > max_score:
                max_score = score[1]
                best_match = x
    return best_match
def match_name_wrapper(args):
    return match_name(*args)
def clean_data(file_path, stamnummer_path):
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')
    
    # Seizoenbegin halen uit Seizoen
    seizoen_split = data['Seizoen'].str.split('-', expand=True)
    data['Seizoen'] = seizoen_split[0]
    
    # Doelpunten splitsen in DoelpuntenVoor en DoelpuntenTegen
    doelpunten_split = data['Doelpunten'].str.split(':', expand=True)
    data['DoelpuntenVoor'] = doelpunten_split[0]
    data['DoelpuntenTegen'] = doelpunten_split[1]

    # Berekenen tweepuntensysteem
    if ":" in data["Punten"]:
        # Splits de 'punten' kolom in twee waarden gebaseerd op de ":" en converteer ze naar integers
        punten_split = data['Punten'].str.split(':', expand=True)

        # Bereken Links_Tweepuntensysteem en Rechts_Tweepuntensysteem op basis van de gesplitste waarden
        data['Links_Tweepuntensysteem'] = punten_split[0].astype(int)
        data['Rechts_Tweepuntensysteem'] = punten_split[1].astype(int)
    else:
        # Bereken Links_Tweepuntensysteem en Rechts_Tweepuntensysteem
        data['Links_Tweepuntensysteem'] = data['AantalGewonnen'] * 2 + data['AantalGelijk']
        data['Rechts_Tweepuntensysteem'] = data['AantalVerloren'] * 2 + data['AantalGelijk']

    # Berekenen driepuntensysteem
    data['Driepuntensysteem'] = data['AantalGewonnen'] * 3 + data['AantalGelijk']

    # Lees de stamnummer data in
    stamnummer_data = pd.read_csv(stamnummer_path, encoding='utf-8')
    stamnummer_names = stamnummer_data['Ploegnaam'].tolist()
    
    # Unieke clubs ophalen voor stamnummer te berekenen
    unique_clubs = data['Club'].unique()
    match_args = [(club, stamnummer_names, 85) for club in unique_clubs]
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(tqdm(executor.map(match_name_wrapper, match_args), total=len(match_args)))
    
    club_to_matched_club = dict(zip(unique_clubs, results))
    
    # Maak een nieuwe kolom 'Stamnummer' gebaseerd op de gevonden matches
    data['Stamnummer'] = data['Club'].apply(lambda club: stamnummer_data.loc[stamnummer_data['Ploegnaam'] == club_to_matched_club[club], 'Stamnummer'].values[0] if club_to_matched_club[club] else None)
    
    # Maak een nieuwe kolom 'Stamnummer' gebaseerd op de gevonden matches
    data['Roepnaam'] = data['Club'].apply(lambda club: stamnummer_data.loc[stamnummer_data['Ploegnaam'] == club_to_matched_club[club], 'Roepnaam'].values[0] if club_to_matched_club[club] else None)
    data['Stamnummer'] = pd.to_numeric(data['Stamnummer'], errors='coerce').fillna(0).astype(int)
    # Kolommen in juiste volgorde zetten
    reordered_list = [
        'Seizoen',
        'Speeldag',
        'Stand',
        'Stamnummer',
        'Roepnaam',
        'AantalGespeeld',
        'AantalGewonnen',
        'AantalGelijk',
        'AantalVerloren',
        'DoelpuntenVoor',
        'DoelpuntenTegen',
        'Doelpuntensaldo',
        'Links_Tweepuntensysteem',
        'Rechts_Tweepuntensysteem',
        'Driepuntensysteem',
    ]
    # Selecteer en ordeneer de gegevens volgens de opgegeven volgorde
    controlled_data = data[reordered_list]
    
    return controlled_data
# Vervang deze paden door jouw werkelijke bestandspaden
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\scraped_data\stand.csv'
stamnummer_path = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\stamnummer\data\stamnummer2.csv'
cleaned_data = clean_data(file_path, stamnummer_path)
# Opslaan van de opgeschoonde data
cleaned_data.to_csv(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv', index=False)