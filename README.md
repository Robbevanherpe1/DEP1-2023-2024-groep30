# DEP-G30

github van DEP-G30 Aalst

## Deel 1

## Voetbalwedstrijdgegevens Scraper

Deze Python-script haalt gegevens op van de website Voetbalkrant om informatie over voetbalwedstrijden in de Belgische Jupiler Pro League van 2002 tot 2023 te extraheren. Het slaat de geëxtraheerde gegevens op in een CSV-bestand met de naam `matches.csv`.

### Vereisten Game_scraper

- BeautifulSoup4
- requests

Je kunt de vereiste bibliotheken installeren met pip:

pip install beautifulsoup4 requests

### Gebruik Game_scraper

1. Kloon of download de repository naar je lokale machine.
2. Open een terminal of opdrachtprompt.
3. Navigeer naar de map met het script.
4. Voer het script uit met Python:
   python fetch_data.py

Het script begint gegevens over wedstrijden op te halen van de website Voetbalkrant en slaat deze op in `matches.csv`. Elke rij in het CSV-bestand vertegenwoordigt een voetbalwedstrijd en bevat de volgende kolommen:

- `Datum`: De datum van de wedstrijd.
- `Thuisploeg`: De naam van de thuisploeg.
- `Score`: De score van de wedstrijd.
- `Uitploeg`: De naam van de uitploeg.

### Aanpassing Game_scraper

Je kunt het script aanpassen door de volgende variabelen aan te passen:

- `base_url`: De basis-URL van de website Voetbalkrant.
- `start_year`: Het startjaar voor het ophalen van wedstrijdgegevens.
- `end_year`: Het eindjaar voor het ophalen van wedstrijdgegevens.

Standaard haalt het script gegevens op van 2002 tot 2022. Je kunt de variabelen `start_year` en `end_year` aanpassen om gegevens op te halen voor een andere reeks jaren.

## Voetbaldoelpunten Data Scraper

Dit Python-script is ontwikkeld om informatie over doelpunten in voetbalwedstrijden van de Belgische Jupiler Pro League van 2007 tot 2023 te extraheren van de website `voetbalkrant.com`. Het opgeslagen bestand, `football_goals.csv`, bevat details van elk doelpunt per wedstrijd.

### Vereisten Goal_scraper

- BeautifulSoup4
- requests
- csv
- re

Je kunt de vereiste bibliotheken installeren met pip:

pip install beautifulsoup4 requests

### Gebruik Goal_scraper

1. Kloon of download de repository naar je lokale machine.
2. Open een terminal of opdrachtprompt.
3. Navigeer naar de map met het script.
4. Voer het script uit met Python:
   python fetch_data.py

Het script verzamelt gegevens over doelpunten van de website `voetbalkrant.com` en slaat deze op in `football_goals.csv`. Elke rij in het CSV-bestand vertegenwoordigt een doelpunt in een voetbalwedstrijd en bevat de volgende kolommen:

- `Jaar`: Het jaar van de wedstrijd.
- `Minuut`: De minuut waarin het doelpunt werd gescoord.
- `Speler`: De naam van de speler die het doelpunt heeft gescoord.

### Aanpassing Goal_scraper

Je kunt het script aanpassen door de volgende variabelen aan te passen:

- `base_url`: De basis-URL van de website `voetbalkrant.com`.
- `start_year`: Het startjaar voor het ophalen van wedstrijdgegevens.
- `end_year`: Het eindjaar voor het ophalen van wedstrijdgegevens.

Standaard haalt het script gegevens op van 2007 tot 2022. Je kunt de variabelen `start_year` en `end_year` aanpassen om gegevens op te halen voor een andere reeks jaren.
