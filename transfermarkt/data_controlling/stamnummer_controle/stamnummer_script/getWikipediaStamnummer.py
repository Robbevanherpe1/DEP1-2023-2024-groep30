import requests
from bs4 import BeautifulSoup
import csv

def fetch_clubs_info(url):
    # Verkrijg de HTML inhoud van de URL
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print("Kon de webpagina niet ophalen")
        return None

def parse_clubs_html(html_content):
    # Parse de HTML om clubinformatie te extraheren
    soup = BeautifulSoup(html_content, 'html.parser')
    ol_tag = soup.find('ol')  # Uitgaande dat de clubs in een <ol> element staan
    list_items = ol_tag.find_all('li') if ol_tag else []
    clubs_info = []

    for index, item in enumerate(list_items, start=1):
        a_tag = item.find('a')
        club_name = a_tag.text if a_tag else 'Onbekende Club'
        clubs_info.append((index, club_name, club_name))

    return clubs_info

def write_to_csv(clubs_info, output_file):
    # Schrijf de clubinformatie naar een CSV-bestand
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Stamnummer', 'Thuisploeg', 'Uitploeg'])  # Bijgewerkte kop
        for club in clubs_info:
            writer.writerow(club)  # Schrijf elke tuple als een rij

# URL van de pagina die je wilt bekijken
url = "https://nl.wikipedia.org/wiki/Lijst_van_voetbalclubs_in_Belgi%C3%AB_naar_stamnummer"
output_file = 'DEP-G30\\transfermarkt\\data_controlling\\stamnummer_controle\\stamnummers_data\\stamnummer.csv'

# Haal op, parse, en schrijf de clubinformatie
html_content = fetch_clubs_info(url)
if html_content:
    clubs_info = parse_clubs_html(html_content)
    write_to_csv(clubs_info, output_file)
    print(f"Gegevens zijn geschreven naar {output_file}")