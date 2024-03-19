import pandas as pd
import re
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

def parse_date(date_str):
    parts = date_str.split(',')
    if len(parts) >= 2:  # Controleren of de lijst voldoende elementen heeft
        day_month_year = parts[1].split()  # Verdelen van de dag, maand en jaar
        day = day_month_year[0][2:]  # Dag uit 'zo4'
        if len(day) == 1:  # 0 toevoegen als dag 1 diget is
            day = '0' + day
        month_str = day_month_year[1]  # Maand uit 'sep.'
        # Maanden afkorten naar hun volledige naam
        months = {
            'jan.': '01',
            'feb.': '02',
            'mrt.': '03',
            'apr.': '04',
            'mei': '05',
            'jun.': '06',
            'jul.': '07',
            'aug.': '08',
            'sep.': '09',
            'okt.': '10',
            'nov.': '11',
            'dec.': '12'
        }
        month = months.get(month_str.lower(), month_str)  # Controleren of de afkorting in de dictionary voorkomt
        year = day_month_year[2]  # Jaar
        return f"{year}/{month}/{day}"
    else:
        return date_str  # Als het formaat niet kan worden geparseerd, retourneren we de originele waarde
    
# Functie om het resultaat op te schonen en alleen de score te behouden
def clean_result(result_str):
    if pd.isna(result_str):  # Controleren of de waarde NaN is
        return result_str
    else:
        parts = str(result_str).split(',')  # Converteer eerst naar een string om te vermijden dat een AttributeError optreedt
        if len(parts) > 1:  # Controleren of de string een komma bevat
            return int(float(parts[0]))  # Converteer naar float en vervolgens naar geheel getal
        else:
            return result_str  # Als er geen komma is, retourneren we de originele waarde

# Functie om de naam van het team op te schonen
def clean_team(team_str):
    return re.sub(r'\([^)]*\)', '', team_str).strip()

# Functie om het tijdstip te formatteren
def parse_time(time_str):
    return time_str.split()[0]  # Het tijdstip is het eerste deel van de string

def clean_data(file_path, stamnummer_path):

    # Load the CSV file
    data = pd.read_csv(file_path)

    # Adjust column names as before
    data.columns = ['Match_ID', 'Seizoen', 'Speeldag', 'Datum', 'Tijdstip', 'Thuisploeg', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg', 'Uitploeg']

    # Omzetten van de datum naar het juiste formaat
    data['Datum'] = data['Datum'].apply(parse_date)

    # Omzetten van het tijdstip naar het juiste formaat
    data['Tijdstip'] = data['Tijdstip'].apply(parse_time)

    # Opschonen van de resultaatkolommen en converteren naar gehele getallen
    data['Resultaat_Thuisploeg'] = data['Resultaat_Thuisploeg'].apply(clean_result)
    data['Resultaat_Uitploeg'] = data['Resultaat_Uitploeg'].apply(clean_result)

    # Verwijderen van NaN-waarden in de resultaatkolommen
    data.dropna(subset=['Resultaat_Thuisploeg', 'Resultaat_Uitploeg'], inplace=True)

    # Converteren naar gehele getallen
    data['Resultaat_Thuisploeg'] = data['Resultaat_Thuisploeg'].astype(int)
    data['Resultaat_Uitploeg'] = data['Resultaat_Uitploeg'].astype(int)

    # Opschonen van de teamnamen
    data['Thuisploeg'] = data['Thuisploeg'].apply(clean_team)
    data['Uitploeg'] = data['Uitploeg'].apply(clean_team)

    # Load the stamnummer data
    stamnummer_data = pd.read_csv(stamnummer_path, encoding='utf-8')
    
    # Match both home and away teams using the corrected column names from your data
    home_teams = data['Thuisploeg'].unique()
    away_teams = data['Uitploeg'].unique()
    
    # Combine unique home and away teams for matching
    unique_teams = set(home_teams) | set(away_teams)
    match_args = [(team, stamnummer_data['Ploegnaam'].tolist(), 85) for team in unique_teams]
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(tqdm(executor.map(match_name_wrapper, match_args), total=len(match_args)))
    
    # Map matched team names to their stamnummer
    team_to_stamnummer_and_roepnaam = {team: (stamnummer_data.loc[stamnummer_data['Ploegnaam'] == matched_team, 'Stamnummer'].values[0], stamnummer_data.loc[stamnummer_data['Ploegnaam'] == matched_team, 'Roepnaam'].values[0]) if matched_team else (None, None) for team, matched_team in zip(unique_teams, results)}
    
    # Create new columns for home and away team stamnummers
    data['Thuisploeg_stamnummer'] = data['Thuisploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (0, None))[0])
    data['Uitploeg_stamnummer'] = data['Uitploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (0, None))[0])
    data['Thuisploeg_roepnaam'] = data['Thuisploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (None, None))[1])
    data['Uitploeg_roepnaam'] = data['Uitploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (None, None))[1])
    
    
    # Ensure stamnummer columns are integers, fill missing with 0
    data[['Thuisploeg_stamnummer', 'Uitploeg_stamnummer']] = data[['Thuisploeg_stamnummer', 'Uitploeg_stamnummer']].fillna(0).astype(int)
    
    return data

# File paths
file_path = 'matches.csv'
stamnummer_path = 'stamnummer2.csv'

cleaned_data = clean_data(file_path, stamnummer_path)

# Save the cleaned data to a new CSV
cleaned_data.to_csv('matches_clean.csv', index=False)




  # Code from the second file
df = pd.read_csv('matches_clean.csv')

  # Data preprocessing steps
df['Seizoen'] = df['Seizoen'].apply(lambda x: x.split('-')[0])
df['Datum'] = pd.to_datetime(df['Datum']).dt.strftime('%Y-%m-%d')

  # Remove seconds from 'Tijdstip'
df['Tijdstip'] = df['Tijdstip'].apply(lambda x: x.split(':')[0] + ':' + x.split(':')[1])

  # Ensure the data is sorted by Match_ID and GoalTijdstip to accurately track score changes
df.sort_values(by=['Match_ID'], inplace=True)

  # Selecting and reordering the necessary columns for final output
columns_order = ['Seizoen', 'Speeldag', 'Datum', 'Tijdstip', 'Match_ID', 'Thuisploeg_stamnummer', 'Thuisploeg_roepnaam', 'Uitploeg_stamnummer', 'Uitploeg_roepnaam', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg']
df_final = df[columns_order].copy()

  # Now, sort df_final by 'Seizoen' and 'Speeldag' before saving
df_final.sort_values(by=['Seizoen', 'Speeldag'], inplace=True)

  # Define the output path where the CSV file will be saved
output_path = 'wedstrijden.csv'

  # Save the DataFrame to CSV with index, headers included, separated by semicolons
df_final.to_csv(output_path, index=False, header=False, sep=';')