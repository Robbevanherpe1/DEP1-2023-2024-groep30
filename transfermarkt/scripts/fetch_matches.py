import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

base_url = "https://www.transfermarkt.be/jupiler-pro-league/spieltagtabelle/wettbewerb/"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'}

startjaar = 2023
eindjaar = datetime.now().year - 1

startspeeldag = 1
eindspeeldag = 50

with open('matches.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['Seizoen','Speeldag','Datum', 'Tijdstip', 'Thuisploeg', 'Resultaat', 'Uitploeg',])
    writer.writeheader()

    for year in range(startjaar, eindjaar + 1):
        for speeldag in range(startspeeldag, eindspeeldag + 1):
            
            url = f"{base_url}BE1?saison_id={year}&spieltag={speeldag}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:

                soup = BeautifulSoup(response.content, 'html.parser')
                trs = soup.select("#yw1 .items tbody tr")

                if not soup.find('option', selected=True, value=str(speeldag)):
                    break  # No more speeldagen for this year

                for row in trs:
                    tds = row.find_all("td")
                    if tds:
                        Datum = tds[1].get_text(strip=True)
                        Tijdstip = tds[2].get_text(strip=True)
                        Thuisploeg = tds[3].get_text(strip=True)
                        Resultaat = tds[4].get_text(strip=True)
                        Uitploeg = tds[5].get_text(strip=True)

                        writer.writerow({
                            'Seizoen': f"{year}-{year+1}",
                            'Speeldag': speeldag,
                            'Datum': Datum,
                            'Tijdstip': Tijdstip,
                            'Thuisploeg': Thuisploeg,
                            'Resultaat': Resultaat,
                            'Uitploeg': Uitploeg
                        })
                print(f"Wedstrijdgegevens voor seizoen {year}, speeldag {speeldag} zijn geschreven.")
            else:
                print(f"Fout bij het ophalen van gegevens voor seizoen {year}, speeldag {speeldag}. Statuscode: {response.status_code}")
