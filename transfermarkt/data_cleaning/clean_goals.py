import pandas as pd
from datetime import datetime, timedelta
import re

def clean_date(d):
    # Extract the numeric part of the date
    match = re.search(r'\d+\s[a-z]+\.\s\d{4}', d, re.IGNORECASE)
    if not match:
        return None  # Return None if no match is found
    
    # Convert Dutch month abbreviations to English for parsing
    dutch_to_english_months = {
        'jan.': 'Jan',
        'feb.': 'Feb',
        'mrt.': 'Mar',
        'apr.': 'Apr',
        'mei': 'May',
        'jun.': 'Jun',
        'jul.': 'Jul',
        'aug.': 'Aug',
        'sep.': 'Sep',
        'okt.': 'Oct',
        'nov.': 'Nov',
        'dec.': 'Dec'
    }
    
    date_str = match.group(0)
    for nl, en in dutch_to_english_months.items():
        date_str = date_str.replace(nl, en)
    
    # Parse the date
    date_obj = datetime.strptime(date_str, '%d %b %Y')
    return date_obj.strftime('%Y/%m/%d')

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

def clean_data(file_path):
    # Load the CSV file
    data = pd.read_csv(file_path)
    
    # Clean the "Datum" column
    data['Datum'] = data['Datum'].apply(clean_date)
    
    # Clean the "Tijdstip" column, converting to time format
    data['Tijdstip'] = pd.to_datetime(data['Tijdstip'].str.replace(' uur', ''), format='%H:%M').dt.time
    
    # Adjust the GoalTijdstip to reflect the actual time a goal is scored
    data['GoalTijdstip'] = data.apply(lambda x: adjust_goal_time(x['Tijdstip'].strftime('%H:%M:%S'), x['GoalTijdstip']), axis=1)
    
    return data

# Example usage
file_path = r'DEP-G30\transfermarkt\data\scraped_data\goals.csv'
cleaned_data = clean_data(file_path)

# Save the cleaned data to a new CSV
cleaned_data.to_csv(r'DEP-G30\transfermarkt\data\cleaned_data\goals_clean.csv', index=False)