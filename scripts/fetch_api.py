import requests
import csv

api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'

response = requests.get(api_url)
data = response.json()

with open('api_data.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Evenement ID', 'Evenementnaam', 'Starttijd', 'Thuisteam', 'Uitteam', 'Marktnaam', 'Uitslag', 'Kansen'])

    for sport in data['tree']:
        for competition in sport.get('competitions', []):
            for event in competition.get('events', []):
                event_id = event.get('id')
                event_name = event.get('name')
                start_time = event.get('starts_at')
                home_team = event.get('home_team')
                away_team = event.get('away_team')

                for market in event.get('markets', []):
                    market_name = market.get('name')
                    for outcome in market.get('outcomes', []):
                        outcome_name = outcome.get('name')
                        odds = outcome.get('odds')
                       
                        writer.writerow([event_id, event_name, start_time, home_team, away_team, market_name, outcome_name, odds])
