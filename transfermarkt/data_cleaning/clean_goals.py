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
    if len(parts) >= 2:
        day_month_year = parts[1].split()
        day = day_month_year[0][2:]
        if len(day) == 1:
            day = '0' + day
        month_str = day_month_year[1]
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
        month = months.get(month_str.lower(), month_str)
        year = day_month_year[2]
        return f"{year}/{month}/{day}"
    else:
        return d

def adjust_goal_time(match_start, goal_minute):
    if pd.isna(goal_minute):
        return match_start
    goal_minute_str = str(goal_minute)
    if '+' in goal_minute_str:
        main_minute, additional = goal_minute_str.split('+')
        total_minutes = int(main_minute) + int(additional)
    else:
        total_minutes = int(float(goal_minute_str))
    match_start_dt = datetime.strptime(match_start, '%H:%M:%S')
    goal_time = match_start_dt + timedelta(minutes=total_minutes)
    return goal_time.strftime('%H:%M')

def clean_and_process_data(file_path, stamnummer_path):
    data = pd.read_csv(file_path)
    data['Match_ID'] = data['Match_ID'].fillna(0).astype(int)
    data['Datum'] = data['Datum'].apply(clean_date)
    data['Tijdstip'] = pd.to_datetime(data['Tijdstip'].str.replace(' uur', ''), format='%H:%M').dt.time
    data['GoalTijdstip'] = data.apply(lambda x: adjust_goal_time(x['Tijdstip'].strftime('%H:%M:%S'), x['GoalTijdstip']), axis=1)
    stamnummer_data = pd.read_csv(stamnummer_path, encoding='utf-8')
    home_teams = data['Thuisploeg'].unique()
    away_teams = data['Uitploeg'].unique()
    unique_teams = set(home_teams) | set(away_teams)
    match_args = [(team, stamnummer_data['Ploegnaam'].tolist(), 85) for team in unique_teams]
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(tqdm(executor.map(match_name_wrapper, match_args), total=len(match_args)))
    team_to_stamnummer_and_roepnaam = {team: (stamnummer_data.loc[stamnummer_data['Ploegnaam'] == matched_team, 'Stamnummer'].values[0], stamnummer_data.loc[stamnummer_data['Ploegnaam'] == matched_team, 'Roepnaam'].values[0]) if matched_team else (None, None) for team, matched_team in zip(unique_teams, results)}
    data['Thuisploeg_stamnummer'] = data['Thuisploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (0, None))[0])
    data['Uitploeg_stamnummer'] = data['Uitploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (0, None))[0])
    data['Thuisploeg_roepnaam'] = data['Thuisploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (None, None))[1])
    data['Uitploeg_roepnaam'] = data['Uitploeg'].apply(lambda team: team_to_stamnummer_and_roepnaam.get(team, (None, None))[1])
    data[['Thuisploeg_stamnummer', 'Uitploeg_stamnummer']] = data[['Thuisploeg_stamnummer', 'Uitploeg_stamnummer']].fillna(0).astype(int)
    
    # Begin processing steps from file 2 here
    data['Seizoen'] = data['Seizoen'].apply(lambda x: x.split('-')[0])
    data['Datum'] = pd.to_datetime(data['Datum']).dt.strftime('%Y-%m-%d')
    data['DoelpuntMinuut'] = data['GoalTijdstip'].apply(lambda x: int(x.split(':')[1]))
    data['Tijdstip'] = pd.to_datetime(data['Tijdstip'], format='%H:%M:%S').dt.strftime('%H:%M')
    data.sort_values(by=['Match_ID', 'GoalTijdstip'], inplace=True)
    data['PreviousThuisScore'] = data.groupby('Match_ID')['StandThuisploeg'].shift(1).fillna(0)
    data['PreviousUitScore'] = data.groupby('Match_ID')['StandUitploeg'].shift(1).fillna(0)
    data['ScoringTeam'] = data.apply(lambda row: row['Thuisploeg_roepnaam'] if row['StandThuisploeg'] > row['PreviousThuisScore'] else row['Uitploeg_roepnaam'], axis=1)
    columns_order = ['Seizoen', 'Speeldag', 'Datum', 'Tijdstip', 'Match_ID', 'Thuisploeg_stamnummer', 'Thuisploeg_roepnaam', 'Uitploeg_stamnummer', 'Uitploeg_roepnaam', 'DoelpuntMinuut', 'GoalTijdstip', 'ScoringTeam', 'StandThuisploeg', 'StandUitploeg']
    df_final = data[columns_order].copy()
    df_final.sort_values(by=['Seizoen', 'Speeldag'], inplace=True)
    
    # Save the processed DataFrame
    output_path = 'goals_clean.csv'
    df_final.to_csv(output_path, index=False, header=False, sep=';')

# File paths
file_path = 'goals.csv'
stamnummer_path = 'stamnummer2.csv'

# Execute the combined function
clean_and_process_data(file_path, stamnummer_path)
