import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import re

# Fetches the URL and returns a BeautifulSoup object
def fetch_url(url):
    try:
        HEADERS = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=HEADERS, timeout=30)
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
        
        data['Seizoen'] = str(seizoen) + '-' + str(seizoen + 1)
        data['Speeldag'] = speeldag
        
        team_names = row.find_all('td', class_='spieltagsansicht-vereinsname')
        if team_names:
            # For Thuisploeg, use the text directly
            data['Thuisploeg'] = re.sub(r"\(.*?\)", "", team_names[0].get_text(strip=True))
            # For Uitploeg, find the <a> tag and use its title attribute
            uitploeg_a_tag = team_names[-1].find('a', title=True)
            if uitploeg_a_tag and uitploeg_a_tag.has_attr('title'):
                data['Uitploeg'] = uitploeg_a_tag['title']
                data['Uitploeg'] = re.sub(r"\(.*?\)", "", data['Uitploeg'])
            else:
                data['Uitploeg'] = "Unknown"

        result_link = row.find('td', class_='spieltagsansicht-ergebnis').find('a', href=True)
        if result_link:
            data['Match_ID'] = extract_match_id(result_link['href'])

        date_time_row = row.find_next_sibling('tr')
        if date_time_row:
            date_time = date_time_row.get_text(strip=True).split('-')
            if len(date_time) >= 2:
                data['Datum'] = date_time[0].strip()
                data['Tijdstip'] = date_time[1].strip()

        goal_rows = box.find_all('tr', class_='spieltagsansicht-aktionen')
        for goal_row in goal_rows:
            goal_data = data.copy()
            goal_details = goal_row.find_all('td')
            if goal_details and len(goal_details) >= 5:
                
                ResultaatMomenteel = goal_details[2].get_text(strip=True)
                if ":" in ResultaatMomenteel:
                    goal_data["StandThuisploeg"], goal_data["StandUitploeg"] = ResultaatMomenteel.split(":")
                else:
                    continue
               
                goal_time_values = [goal_details[i].get_text(strip=True).rstrip("'") for i in [1, 3]]
                goal_data['GoalTijdstip'] = next((time for time in goal_time_values if time), None)

                goal_scorers = [goal_details[i].find('a', title=True) for i in [0, 4]]
                goal_data['GoalScorer'] = next((scorer['title'] for scorer in goal_scorers if scorer is not None), None)
                
                # Determine the scoring team based on the last goal
                if goal_details[0].find('a', title=True):  # Als de doelpuntmaker in de eerste kolom staat, scoort de thuisploeg
                    goal_data['NaamScorendePloeg'] = goal_data['Thuisploeg']
                elif goal_details[4].find('a', title=True):  # Als de doelpuntmaker in de laatste kolom staat, scoort de uitploeg
                    goal_data['NaamScorendePloeg'] = goal_data['Uitploeg']
                
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
    URL = 'https://www.transfermarkt.be/jupiler-pro-league/spieltag/wettbewerb/BE1/plus/?saison_id='
    STARTJAAR = 1960
    EINDJAAR = datetime.now().year
    
    all_matches = []

    for jaar in range(STARTJAAR, EINDJAAR):
        print(f"Processing data for the season: {jaar}-{jaar+1}")
        speeldag = 1
        while True:
            url = f'{URL}{jaar}&spieltag={speeldag}'
            soup = fetch_url(url)
            if not soup.find('option', selected=True, value=str(speeldag)):
                break  # geen speeldag meer voor dit seizoen
            matches = process_all_boxes(soup, jaar, speeldag)
            all_matches.extend(matches)
            speeldag += 1

    # Write data to CSV
    with open(r'DEP-G30\transfermarkt\data\scraped_data\goals.csv', 'w', newline='', encoding='utf-8') as file:
        fieldnames = ['Match_ID', 'Seizoen', 'Speeldag', 'Datum', 'Tijdstip', 'Thuisploeg', 'Uitploeg','NaamScorendePloeg',
                       'GoalTijdstip', 'GoalScorer', 'StandThuisploeg', 'StandUitploeg']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for match in all_matches:
            writer.writerow(match)

    print("Data scraping completed. Check voetbal_data.csv file.")

main()