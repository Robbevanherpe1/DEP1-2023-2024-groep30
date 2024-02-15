import requests
from bs4 import BeautifulSoup

# Define the base URL
base_url = "https://www.voetbalkrant.com/belgie/jupiler-pro-league/geschiedenis/"

# Define the range of years you want to scrape
start_year = 2004
end_year = 2005  # Adjust this to the desired end year

# Iterate over each year
for year in range(start_year, end_year + 1):
    url = f"{base_url}{year}-{year + 1}/wedstrijden"
    response = requests.get(url)
    if response.status_code == 200:
        html_content = response.content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all table rows containing match data
        match_rows = soup.find_all('tr', class_='table-active')

        # Initialize a list to store match information
        matches = []

        # Extract match data from each row
        for row in match_rows:
            cells = row.find_all('td')
            match_date = cells[0].text.strip()
            home_team = cells[1].text.strip()
            score = cells[2].text.strip()
            away_team = cells[3].text.strip()

            match_info = {
                'Date': match_date,
                'Home Team': home_team,
                'Score': score,
                'Away Team': away_team
            }

            matches.append(match_info)

        # Print the extracted match data
        print(f"Match data for {year}-{year + 1}:")
        for match in matches:
            print(match)
        print("\n")
    else:
        print(f"Failed to retrieve data for {year}-{year + 1}. Status code: {response.status_code}")
