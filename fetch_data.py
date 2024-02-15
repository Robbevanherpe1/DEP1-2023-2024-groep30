import requests
from bs4 import BeautifulSoup

url = 'https://www.voetbalkrant.com/belgie/jupiler-pro-league/geschiedenis/2002-2003/wedstrijden'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# alle divs met klasse "table-responsive" zoeken
table_responsive_divs = soup.find_all('div', class_='table-responsive')

all_matches = []

for div in table_responsive_divs:
    #alle rijen vinden met match data
    match_rows = div.find_all('tr', class_='table-active')
    
    # iedere rij data uithalen
    for row in match_rows:
        cells = row.find_all('td')
        match_date = cells[0].text.strip()
        home_team = cells[1].text.strip()
        score = cells[2].text.strip()
        away_team = cells[3].text.strip()

        match_info = {
            'Date': match_date,
            'Home Team': home_team,
            'Score': score,
            'Away Team': away_team
        }

        all_matches.append(match_info)

# uitprintenvan data
for match in all_matches:
    print(match)
