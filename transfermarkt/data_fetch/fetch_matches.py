import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re


URL = "https://www.transfermarkt.be/jupiler-pro-league/spieltag/wettbewerb/BE1/plus/"
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'}

STARTJAAR = 1960
EINDJAAR = datetime.now().year - 1

STARTSPEELDAG = 1
EINDSPEELDAG = 50

with open(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\scraped_data\matches.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['Match_ID', 'Seizoen', 'Speeldag', 'Datum', 'Tijdstip', 'Thuisploeg', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg', 'Uitploeg'])
    writer.writeheader()

    for jaar in range(STARTJAAR, EINDJAAR + 1):
        for speeldag in range(STARTSPEELDAG, EINDSPEELDAG - 1):

            url = f"{URL}?saison_id={jaar}&spieltag={speeldag}"
            response = requests.get(url, headers=HEADERS)

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

                    # Extraheren van de thuisploeg en uitploeg:
                    # Thuisploeg
                    thuisploeg_tag = match.find('td', class_='rechts hauptlink no-border-rechts hide-for-small spieltagsansicht-vereinsname')
                    thuisploeg = thuisploeg_tag.get_text(strip=True) if thuisploeg_tag else None
                    if thuisploeg:
                        thuisploeg = re.sub(r"\d|\(.*?\)", "", thuisploeg).strip()

                    # Uitploeg
                    uitploeg_tag = match.find('td', class_='hauptlink no-border-links no-border-rechts hide-for-small spieltagsansicht-vereinsname')
                    uitploeg = uitploeg_tag.get_text(strip=True) if uitploeg_tag else None
                    if uitploeg:
                        uitploeg = re.sub(r"\d|\(.*?\)", "", uitploeg).strip()

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
                    match_id_element = match.find('a', title=lambda title: title in ['Wedstrijdverslag', 'Voorbeschouwing'])  # Zoek naar de anker tag met de titel 'Wedstrijdverslag' of 'Voorbeschouwing'
                    if match_id_element:  # Controleer of het element bestaat
                        match_id_url = match_id_element['href']  # Haal de waarde van het 'href'-attribuut op
                        match_id = match_id_url.split('/')[-1]  # Split de URL op '/' en neem het laatste deel, dat de Match-ID bevat
                    else:
                        match_id = None
                   
                    # Check if any of the fields contain data before writing the row
                    if any([match_id, datum, tijdstip, thuisploeg, score_thuisploeg, score_uitploeg, uitploeg]):
                        writer.writerow({
                            'Match_ID': match_id,
                            'Seizoen': f"{jaar}-{jaar+1}",
                            'Speeldag': speeldag,
                            'Datum': datum.strip() if datum else None,
                            'Tijdstip': tijdstip.strip() if tijdstip else None,
                            'Thuisploeg': thuisploeg,
                            'Resultaat_Thuisploeg': score_thuisploeg,
                            'Resultaat_Uitploeg': score_uitploeg,
                            'Uitploeg': uitploeg,
                        })
                print(f"Wedstrijdgegevens voor seizoen {jaar}-{jaar+1}, speeldag {speeldag} zijn geschreven.")
            else:
                print(f"Fout bij het ophalen van gegevens voor seizoen {jaar}-{jaar+1}, speeldag {speeldag}. Statuscode: {response.status_code}")
