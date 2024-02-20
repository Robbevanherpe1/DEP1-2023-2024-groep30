import requests
import csv

# API endpoint
api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'

# Make a GET request to the API
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse JSON response
    api_data = response.json()

    # Extract teams and their scoring points
team_goals = []
for filter_data in api_data['tree'][0]['competitions'][0]['market_filters']:
    if 'labels' in filter_data:
        team_goals.append([filter_data['labels'][0], filter_data['labels'][1], filter_data['labels'][1]])

# Writing the data to CSV
with open('teams_goals.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Team 1', 'Goals', 'Team 2'])
    writer.writerows(team_goals)