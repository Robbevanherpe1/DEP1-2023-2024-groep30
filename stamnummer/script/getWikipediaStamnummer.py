import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
from fuzzywuzzy import fuzz
from tqdm.auto import tqdm

def fetch_clubs_info(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print("Kon de webpagina niet ophalen")
        return None

def parse_clubs_html(html_content, unique_clubs):
    soup = BeautifulSoup(html_content, 'html.parser')
    clubs_info = []

    tables = soup.find_all('table', {'class': 'wikitable sortable'})
    for table in tables:
        rows = table.find_all('tr')
        for row in tqdm(rows[1:], desc="Verwerken van clubs in tabel"):
            cols = row.find_all('td')
            if len(cols) > 1:
                club_name = cols[1].text.strip()
                stamnummer = cols[0].text.strip()
                best_match_score = max(fuzz.partial_ratio(club_name, unique_club) for unique_club in unique_clubs)
                if best_match_score >= 80:
                    clubs_info.append((stamnummer, club_name, club_name))

    if not tables:
        ol_tag = soup.find('ol')
        list_items = ol_tag.find_all('li') if ol_tag else []
        for index, item in enumerate(tqdm(list_items, desc="Verwerken van clubs in lijst"), start=1):
            a_tag = item.find('a')
            club_name = a_tag.text if a_tag else 'Onbekende Club'
            best_match_score = max(fuzz.partial_ratio(club_name, unique_club) for unique_club in unique_clubs)
            if best_match_score >= 80:
                clubs_info.append((str(index), club_name, club_name))

    return clubs_info

def read_unique_clubs(csv_file):
    df = pd.read_csv(csv_file)
    return df['Club'].tolist()

def write_to_csv(clubs_info, output_file):
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Stamnummer', 'Thuisploeg', 'Uitploeg'])
        for club in tqdm(clubs_info, desc="Schrijven naar CSV"):
            writer.writerow(club)

unique_clubs_csv = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\stamnummer\data\unieke_ploegen.csv'
unique_clubs = read_unique_clubs(unique_clubs_csv)

url = "https://nl.wikipedia.org/wiki/Lijst_van_voetbalclubs_in_Belgi%C3%AB_naar_stamnummer"
output_file = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\stamnummer\data\stamnummer.csv'

html_content = fetch_clubs_info(url)
if html_content:
    clubs_info = parse_clubs_html(html_content, unique_clubs)
    write_to_csv(clubs_info, output_file)
    print(f"Gegevens zijn geschreven naar {output_file}")
