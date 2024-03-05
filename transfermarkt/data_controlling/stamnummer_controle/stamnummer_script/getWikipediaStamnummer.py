import requests
from bs4 import BeautifulSoup
import csv

def fetch_clubs_info(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print("Failed to retrieve the webpage")
        return None

def parse_clubs_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    ol_tag = soup.find('ol')  # Assuming the clubs are listed in an <ol> element
    list_items = ol_tag.find_all('li') if ol_tag else []
    clubs_info = []

    for index, item in enumerate(list_items, start=1):
        a_tag = item.find('a')
        club_name = a_tag.text if a_tag else 'Unknown Club'
        clubs_info.append((index, club_name,club_name))

    return clubs_info

def write_to_csv(clubs_info, output_file):
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Stamnummer', 'Thuisploeg', 'Uitploeg'])  # Updated header
        for club in clubs_info:
            writer.writerow(club)  # Write each tuple as a row

# URL of the page you're interested in
url = "https://nl.wikipedia.org/wiki/Lijst_van_voetbalclubs_in_Belgi%C3%AB_naar_stamnummer"
output_file = 'DEP-G30\transfermarkt\data_controlling\stamnummer_controle\stamnummers_data\stamnummer.csv'

# Fetch, parse, and write the club info
html_content = fetch_clubs_info(url)
if html_content:
    clubs_info = parse_clubs_html(html_content)
    write_to_csv(clubs_info, output_file)
    print(f"Data has been written to {output_file}")