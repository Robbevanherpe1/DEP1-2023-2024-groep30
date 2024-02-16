import requests
from bs4 import BeautifulSoup
import csv

urls = [
    "https://www.voetbalkrant.com/belgie/jupiler-pro-league/geschiedenis/",
    "https://www.voetbalkrant.com/duitsland/bundesliga/geschiedenis/",
    "https://www.voetbalkrant.com/engeland/premier-league/geschiedenis/",
    "https://www.voetbalkrant.com/frankrijk/ligue-1/geschiedenis/",
    "https://www.voetbalkrant.com/italie/serie-a/geschiedenis/",
    "https://www.voetbalkrant.com/nederland/nederlandse-eredivisie/geschiedenis/",
    "https://www.voetbalkrant.com/spanje/la-liga/geschiedenis/"
]

start_year = 2002
end_year = 2022

def fetch_matches(base_url):
    with open('matches.csv', mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['Date', 'Home Team', 'Score', 'Away Team'])
        writer.writeheader()

        for year in range(start_year, end_year + 1):
            url = f"{base_url}{year}-{year + 1}/wedstrijden"
            response = requests.get(url)
            if response.status_code == 200:
                html_content = response.content
                soup = BeautifulSoup(html_content, 'html.parser')

                match_rows = soup.find_all('tr', class_='table-active')

                for row in match_rows:
                    cells = row.find_all('td')
                    match_date = cells[0].text.strip()
                    home_team = cells[1].text.strip()
                    score = cells[2].text.strip()
                    away_team = cells[3].text.strip()

                    writer.writerow({'Date': match_date, 'Home Team': home_team, 'Score': score, 'Away Team': away_team})

                print(f"Match data for {year}-{year + 1} has been written to matches.csv.")
            else:
                print(f"Failed to retrieve data for {year}-{year + 1}. Status code: {response.status_code}")

for url in urls:
    fetch_matches(url)