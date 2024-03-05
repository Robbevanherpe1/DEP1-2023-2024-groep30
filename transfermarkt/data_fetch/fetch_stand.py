import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

URL = "https://www.transfermarkt.be/jupiler-pro-league/spieltagtabelle/wettbewerb/"
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'}

STARTJAAR = 1960
EINDJAAR = datetime.now().year - 1

STARTSPEELDAG = 1
EINDSPEELDAG = 50

with open(r'DEP-G30\transfermarkt\data\scraped_data\stand.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=   [  
                                                'Seizoen', 'Speeldag', 'Stand', 'Club',
                                                'AantalGespeeld', 'AantalGewonnen', 'AantalGelijk', 
                                                'AantalVerloren', 'Doelpunten', 'Doelpuntensaldo', 'Punten'
                                                ])
    writer.writeheader()

    for jaar in range(STARTJAAR, EINDJAAR + 1):
        for speeldag in range(STARTSPEELDAG, EINDSPEELDAG + 1):
            
            url = f"{URL}BE1?saison_id={jaar}&spieltag={speeldag}"
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:

                soup = BeautifulSoup(response.content, 'html.parser')
                trs = soup.select("#yw1 .items tbody tr")

                if not soup.find('option', selected=True, value=str(speeldag)):
                    break  # geen speeldagen meer beschikbaar voor dit seizoen

                for row in trs:
                    tds = row.find_all("td")
                    if tds:
                        Stand = tds[0].get_text(strip=True)
                        Club = tds[2].get_text(strip=True)
                        AantalGespeeld = tds[3].get_text(strip=True)
                        AantalGewonnen = tds[4].get_text(strip=True)
                        AantalGelijk = tds[5].get_text(strip=True)
                        AantalVerloren = tds[6].get_text(strip=True)
                        Doelpunten = tds[7].get_text(strip=True)
                        Doelpuntensaldo = tds[8].get_text(strip=True)
                        Punten = tds[9].get_text(strip=True)

                        writer.writerow({
                            'Seizoen': f"{jaar}-{jaar+1}",
                            'Speeldag': speeldag,
                            'Stand': Stand, 
                            'Club': Club,
                            'AantalGespeeld': AantalGespeeld,
                            'AantalGewonnen': AantalGewonnen,
                            'AantalGelijk': AantalGelijk,
                            'AantalVerloren': AantalVerloren,
                            'Doelpunten': Doelpunten,
                            'Doelpuntensaldo': Doelpuntensaldo,
                            'Punten': Punten
                        })
                print(f"Wedstrijdgegevens voor seizoen {jaar}-{jaar+1}, speeldag {speeldag} zijn geschreven.")
            else:
                print(f"Fout bij het ophalen van gegevens voor seizoen {jaar}-{jaar+1}, speeldag {speeldag}. Statuscode: {response.status_code}")
