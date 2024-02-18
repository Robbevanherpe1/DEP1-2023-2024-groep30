import requests
from bs4 import BeautifulSoup
import csv
import re

def fetch_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def get_match_links(year_page_soup):
    links = []
    if year_page_soup:
        for td in year_page_soup.find_all('td', class_='text-center'):
            a = td.find('a', href=True)
            if a:
                links.append(a['href'])
    return links

def get_goal_details(match_page_soup):
    goals = []
    if match_page_soup:
        more_info = match_page_soup.find('div', class_='moreInfo')
        if more_info:
            for row in more_info.find_all('div', class_='row'):
                for small in row.find_all('small'):
                    goal_info = {'minute': None, 'player_goal': None, 'player_assist': None}
                    goal_minute = re.search(r'\d+', small.get_text())
                    goal_info['minute'] = goal_minute.group(0) if goal_minute else None

                    players = small.find_all('a', class_='text-white')
                    for player in players:
                        player_name = player.get_text().strip()
                        
                        if f"({player_name})" in small.get_text():
                            goal_info['player_assist'] = player_name
                        else:
                            goal_info['player_goal'] = player_name

                    goals.append(goal_info)
    return goals


data = []
base_url = 'https://www.voetbalkrant.com'
match_id = 885

for year in range(2005, 2023):
    print(f"Processing year: {year}")
    year_url = f'{base_url}/belgie/jupiler-pro-league/geschiedenis/{year}-{year+1}/wedstrijden'
    year_soup = fetch_url(year_url)
    match_links = get_match_links(year_soup)
    
    
    for match_link in match_links:
        match_id += 1  
        match_soup = fetch_url(base_url + match_link)
        match_goals = get_goal_details(match_soup)
        for goal in match_goals:
            data.append({
                'ID': match_id,
                'Year': year,
                'Minute': goal['minute'],
                'player_goal': goal['player_goal'],
                'player_Assist': goal['player_assist']
            })

with open('football_goals.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['ID', 'Year', 'Minute', 'player_goal','player_Assist'])
    writer.writeheader()
    writer.writerows(data)

print("Data scraping complete. Check the football_goals.csv file.")
