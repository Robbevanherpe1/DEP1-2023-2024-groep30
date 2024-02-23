import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import re

# Fetches the URL and returns a BeautifulSoup object
def fetch_url(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

# Extracts match ID from the URL
def extract_match_id(url):
    match_id = re.search(r'/spielbericht/index/spielbericht/(\d+)', url)
    return match_id.group(1) if match_id else None

# Extracts match data from a given box
def get_match_data(box, seizoen, speeldag):
    match_data = []
    rows = box.find_all('tr', class_='table-grosse-schrift')
    for row in rows:
        data = {}
        
        data['Seizoen'] = str(seizoen) + '/' + str(seizoen + 1)
        data['Speeldag'] = speeldag
        
        team_names = row.find_all('td', class_='spieltagsansicht-vereinsname')
        if team_names:
            data['Home_Team'] = re.sub(r"\(.*?\)", "", team_names[0].get_text(strip=True))
            data['Away_Team'] = re.sub(r"\(.*?\)", "", team_names[-1].get_text(strip=True))

        result_link = row.find('td', class_='spieltagsansicht-ergebnis').find('a', href=True)
        if result_link:
            data['Match_ID'] = extract_match_id(result_link['href'])

        date_time_row = row.find_next_sibling('tr')
        if date_time_row:
            date_time = date_time_row.get_text(strip=True).split('-')
            if len(date_time) >= 2:
                data['Date'] = date_time[0].strip()
                data['Time'] = date_time[1].strip()

        goal_rows = box.find_all('tr', class_='spieltagsansicht-aktionen')
        for goal_row in goal_rows:
            goal_data = data.copy()
            goal_details = goal_row.find_all('td')
            if goal_details and len(goal_details) >= 5:
                
                goal_data['Current_Score'] = goal_details[2].get_text(strip=True)
               
                goal_time_values = [goal_details[i].get_text(strip=True).rstrip("'") for i in [1, 3]]
                goal_data['Goal_Time'] = next((time for time in goal_time_values if time), None)

                goal_scorers = [goal_details[i].find('a', title=True) for i in [0, 4]]
                goal_data['Scorer'] = next((scorer['title'] for scorer in goal_scorers if scorer is not None), None)
                
                
                
                match_data.append(goal_data)

    return match_data

# Processes all boxes to get match data
def process_all_boxes(soup, seizoen, speeldag):
    all_matches = []
    boxes = soup.find_all('div', class_='box')[1:]  # Skip the first box
    for box in boxes:
        match_data = get_match_data(box, seizoen, speeldag)
        all_matches.extend(match_data)
    return all_matches

# Main function to process data for each year and speeldag
def main():
    url_base = 'https://www.transfermarkt.be/jupiler-pro-league/spieltag/wettbewerb/BE1/plus/?saison_id='
    year_start = 1960
    year_end =  datetime.now().year
    
    all_matches = []

    for year in range(year_start, year_end):
        print(f"Processing data for the year: {year}")
        speeldag = 1
        while True:
            url = f'{url_base}{year}&spieltag={speeldag}'
            soup = fetch_url(url)
            if not soup.find('option', selected=True, value=str(speeldag)):
                break  # No more speeldagen for this year
            matches = process_all_boxes(soup, year, speeldag)
            all_matches.extend(matches)
            speeldag += 1

    # Write data to CSV
    with open('voetbal_data.csv', 'w', newline='', encoding='utf-8') as file:
        fieldnames = ['Match_ID', 'Seizoen', 'Speeldag', 'Date', 'Time', 'Home_Team', 'Away_Team', 'Current_Score', 'Goal_Time', 'Scorer']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for match in all_matches:
            writer.writerow(match)

    print("Data scraping completed. Check voetbal_data.csv file.")

main()