import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

base_url = "https://www.voetbalkrant.com/belgie/jupiler-pro-league/geschiedenis/"
start_year = 2002
end_year = datetime.now().year - 1

match_id = 1

with open('matches.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['ID', 'Startjaar', 'Eindjaar', 'Datum', 'Tijd', 'Thuisploeg', 'Score', 'Uitploeg'])
    writer.writeheader()

    for year in range(start_year, end_year + 1):
        url = f"{base_url}{year}-{year + 1}/wedstrijden"
        response = requests.get(url,timeout=30)
        if response.status_code == 200:
            html_content = response.content
            soup = BeautifulSoup(html_content, 'html.parser')

            match_rows = soup.find_all('tr', class_='table-active')

            for row in match_rows:
                cells = row.find_all('td')
                match_date_time = cells[0].text.strip()
                # splitsen datum en uur
                match_date, match_time = match_date_time.split(' ', 1)
                home_team = cells[1].text.strip()
                score = cells[2].text.strip()
                away_team = cells[3].text.strip()

                writer.writerow({
                    'ID': match_id,
                    'Startjaar': year, 
                    'Eindjaar': year + 1, 
                    'Datum': match_date, 
                    'Tijd': match_time, 
                    'Thuisploeg': home_team, 
                    'Score': score, 
                    'Uitploeg': away_team
                })

                match_id += 1 # Verhoog ID

            print(f"Wedstrijdgegevens voor {year}-{year + 1} zijn geschreven naar matches.csv.")
        else:
            print(f"Fout bij het ophalen van gegevens voor {year}-{year + 1}. Statuscode: {response.status_code}")
