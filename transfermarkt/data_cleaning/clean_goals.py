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
    output_path = 'combined_output.csv'
    df_final.to_csv(output_path, index=False, header=False, sep=';')

    # Perform additional operations on the combined output
    df = pd.read_csv(output_path, sep=';', header=None)
    
    # Convert the time columns to datetime objects
    df[3] = pd.to_datetime(df[3], format='%H:%M', errors='coerce')
    df[10] = pd.to_datetime(df[10], format='%H:%M', errors='coerce')
    
    # Calculate the difference between goal time and start match time
    df['goal_time_diff'] = (df[3] - df[10]).dt.total_seconds() / 60
    
    # Update the goal minute if it doesn't match the actual goal time
    df[13] = df.apply(lambda x: int(x[13]) if pd.isnull(x[10]) or x[12] == x['goal_time_diff'] else int((x[10] + pd.Timedelta(minutes=abs(x[12]))).strftime('%M')), axis=1)
    
    # Format the time columns
    df[3] = df[3].dt.strftime('%H:%M')
    df[10] = df[10].dt.strftime('%H:%M')
    
    # Save the last given row
    last_row = df.iloc[-1].copy()
    
    # Append a new row containing the absolute difference value
    abs_diff_row = df['goal_time_diff'].abs().tolist()
    df.loc[len(df)] = df.iloc[0].apply(lambda x: abs_diff_row[0] if pd.isnull(x) else '')
    df.loc[len(df)-1, 12] = abs_diff_row[0]
    
    # Restore the last given row
    df.iloc[-1] = last_row
    
    # Select only the last column
    last_added_column = df.iloc[:, -1]
    
    # Save the last column as a CSV file
    last_added_column.to_csv("final_output.csv", sep=';', index=False, header=False)

    # Read the first input CSV file
    df1 = pd.read_csv(output_path, sep=';', header=None)

    # Read the second input CSV file
    df2 = pd.read_csv("final_output.csv", header=None)

    df2_abs = df2.abs()

    # Replace the values in column 9 of df1 with the absolute values from df2
    df1[9] = df2_abs[0].astype(str).str.replace('\.0', '', regex=True)

    # Save the updated dataframe to a new CSV file
    df1.to_csv("clean_goals.csv", sep=';', index=False, header=False)

# File paths
file_path = 'goals.csv'
stamnummer_path = 'stamnummer2.csv'

# Execute the combined function
clean_and_process_data(file_path, stamnummer_path)
