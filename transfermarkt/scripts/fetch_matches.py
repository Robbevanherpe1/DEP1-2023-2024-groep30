import requests
import os
from bs4 import BeautifulSoup
import csv
from datetime import datetime

base_url = "https://www.transfermarkt.be/jupiler-pro-league/spieltag/wettbewerb/BE1/plus/"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'}
directory = '/transfermarkt/data'

csv_path = os.path.join(directory, 'alle_matches.csv')


startjaar = 2000  # Beginjaar
eindjaar = datetime.now().year  # Huidig jaar

with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['Seizoen','Speeldag','Datum', 'Tijdstip', 'Naam thuisploeg', 'Resultaat', 'Naam uitploeg',])
    writer.writeheader()

    for year in range(startjaar, eindjaar + 1):
        for speeldag in range(1, 40):  # Maximaal aantal speeldagen per seizoen (typisch ongeveer 38-40)
            url = f"{base_url}?saison_id={year}&spieltag={speeldag}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                match_rows = soup.select(".box")  # Selecteer de juiste container voor elke wedstrijd
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

                     # Match-ID
                    match_id_tag = match.find('a', class_='ergebnis-link')
                    match_id = match_id_tag['href'].split('/')[-1] if match_id_tag else None

                    writer.writerow({
                        'Seizoen': year,
                        'Speeldag': speeldag,
                        'Datum': datum.strip() if datum else None,
                        'Tijdstip': tijdstip.strip() if tijdstip else None,
                        'Naam thuisploeg': thuisploeg,
                        'Resultaat': resultaat,
                        'Naam uitploeg': uitploeg,


                    })
                    print(f"Wedstrijdgegevens voor seizoen {year}, speeldag {speeldag} zijn geschreven.")
            else:
                print(f"Fout bij het ophalen van gegevens voor seizoen {year}, speeldag {speeldag}. Statuscode: {response.status_code}")