import pandas as pd
from datetime import datetime, timedelta
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

def clean_date(d):
    parts = d.split(',')
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
        return d


def adjust_goal_time(match_start, goal_minute):
    # Check for NaN values and handle them
    if pd.isna(goal_minute):
        # Handle NaN values according to your preference
        # For example, return match start time as a placeholder
        return match_start

    # Convert goal_minute to string to ensure it's iterable
    goal_minute_str = str(goal_minute)

    # Check if goal_minute_str contains additional time
    if '+' in goal_minute_str:
        main_minute, additional = goal_minute_str.split('+')
        total_minutes = int(main_minute) + int(additional)
    else:
        total_minutes = int(float(goal_minute_str))  # Convert string to float first, then to int, to handle cases like '45.0'

    # Convert match start time to a datetime object
    match_start_dt = datetime.strptime(match_start, '%H:%M:%S')
    # Calculate the goal time by adding the total_minutes to the match_start time
    goal_time = match_start_dt + timedelta(minutes=total_minutes)
    # Return the goal time formatted as HH:MM
    return goal_time.strftime('%H:%M')

def clean_data(file_path, stamnummer_path):
    # Load the CSV file
    data = pd.read_csv(file_path)

    # Convert Match_ID from float to integer
    data['Match_ID'] = data['Match_ID'].fillna(0).astype(int)
    
    # Clean the "Datum" column
    data['Datum'] = data['Datum'].apply(clean_date)
    
    # Clean the "Tijdstip" column, converting to time format
    data['Tijdstip'] = pd.to_datetime(data['Tijdstip'].str.replace(' uur', ''), format='%H:%M').dt.time
    
    # Adjust the GoalTijdstip to reflect the actual time a goal is scored
    data['GoalTijdstip'] = data.apply(lambda x: adjust_goal_time(x['Tijdstip'].strftime('%H:%M:%S'), x['GoalTijdstip']), axis=1)
    
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
    team_to_stamnummer = {team: stamnummer_data.loc[stamnummer_data['Ploegnaam'] == matched_team, 'Stamnummer'].values[0] if matched_team else None for team, matched_team in zip(unique_teams, results)}
    
    # Create new columns for home and away team stamnummers
    data['Thuisploeg_stamnummer'] = data['Thuisploeg'].apply(lambda team: team_to_stamnummer.get(team, None))
    data['Uitploeg_stamnummer'] = data['Uitploeg'].apply(lambda team: team_to_stamnummer.get(team, None))
    
    # Ensure stamnummer columns are integers, fill missing with 0
    data[['Thuisploeg_stamnummer', 'Uitploeg_stamnummer']] = data[['Thuisploeg_stamnummer', 'Uitploeg_stamnummer']].fillna(0).astype(int)
    
    return data

# File paths
file_path = 'goals.csv'
stamnummer_path = 'stamnummer2.csv'

cleaned_data = clean_data(file_path, stamnummer_path)

# Save the cleaned data to a new CSV
cleaned_data.to_csv('goals_clean10.csv', index=False)