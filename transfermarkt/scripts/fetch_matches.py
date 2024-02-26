
import requests
import os
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re


base_url = "https://www.transfermarkt.be/jupiler-pro-league/spieltag/wettbewerb/BE1/plus/"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'}

startjaar = 2023  # Beginjaar
eindjaar = datetime.now().year - 1  # Huidig jaar

startspeeldag = 1
eindspeeldag = 50

with open('matches.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['Seizoen', 'Speeldag', 'Datum', 'Tijdstip', 'Naam thuisploeg', 'Resultaat thuisploeg', 'Resultaat uitploeg', 'Naam uitploeg','Match-ID'])
    writer.writeheader()

    for year in range(startjaar, eindjaar + 1):
        for speeldag in range(startspeeldag, eindspeeldag - 1):  # Maximaal aantal speeldagen per seizoen (typisch ongeveer 38-40)
            url = f"{base_url}?saison_id={year}&spieltag={speeldag}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                match_rows = soup.select(".box")  # Selecteer de juiste container voor elke wedstrijd

                if not soup.find('option', selected=True, value=str(speeldag)):
                    break  # geen speeldagen meer beschikbaar voor dit seizoen

                for match in match_rows:

                    # Thuisploeg
                    thuisploeg = match.find('td', class_='rechts hauptlink no-border-rechts hide-for-small spieltagsansicht-vereinsname')
                    thuisploeg = thuisploeg.get_text(strip=True) if thuisploeg else None

                    # Uitploeg
                    uitploeg = match.find('td', class_='hauptlink no-border-links no-border-rechts hide-for-small spieltagsansicht-vereinsname')
                    uitploeg = uitploeg.get_text(strip=True) if uitploeg else None

                    # Datum en tijdstip van de match
                    datum_tijdstip_tag = match.find('td', class_='zentriert no-border')
                    if datum_tijdstip_tag:
                        datum_tijdstip = datum_tijdstip_tag.get_text(strip=True)
                        datum, tijdstip = datum_tijdstip.split('-')
                    else:
                        datum = None
                        tijdstip = None

                    # Resultaat van de wedstrijd
                    resultaat_element = match.find('span', class_='matchresult finished')
                    resultaat = resultaat_element.get_text(strip=True) if resultaat_element else None

                    # Scores splitsen
                    if resultaat:
                        scores = resultaat.split(':')
                        if len(scores) == 2:
                            score_thuisploeg = scores[0].strip()
                            score_uitploeg = scores[1].strip()
                        else:
                            score_thuisploeg = None
                            score_uitploeg = None
                    else:
                        score_thuisploeg = None
                        score_uitploeg = None

                    # Match-ID
                    match_id_tag = match.find('a', class_='liveLink')
                    match_id = match_id_tag['href'].split('/')[-1] if match_id_tag else None

                    writer.writerow({
                        'Match_ID': match_id,
                        'Seizoen': f"{jaar}-{jaar+1}",
                        'Speeldag': speeldag,
                        'Datum': datum.strip() if datum else None,
                        'Tijdstip': tijdstip.strip() if tijdstip else None,
                        'Naam thuisploeg': thuisploeg,
                        'Resultaat thuisploeg': score_thuisploeg,
                        'Resultaat uitploeg': score_uitploeg,
                        'Naam uitploeg': uitploeg,
                        'Match-ID': match_id,
                    })
                print(f"Wedstrijdgegevens voor seizoen {jaar}-{jaar+1}, speeldag {speeldag} zijn geschreven.")
            else:
                print(f"Fout bij het ophalen van gegevens voor seizoen {jaar}-{jaar+1}, speeldag {speeldag}. Statuscode: {response.status_code}")