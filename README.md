# DEP-G30 [![Codacy Badge](https://app.codacy.com/project/badge/Grade/43a112dd62f5482d9da2271a30a389a0)](https://app.codacy.com/gh/Robbevanherpe1/DEP1-2023-2024-groep30/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

## Groepsleden

Ayman Thomas <ayman.thomas@student.hogent.be>

Matteo De Moor <matteo.de.moor@student.hogent.be>

Robbe Van Herpe <robbe.vanherpe@student.hogent.be>

## Jira-project

link <https://dep-g30.atlassian.net/jira/software/c/projects/SEP2324G30/boards/2/>

## Omschrijving

zie later

## Opdracht

### Deel 1

Hier maken we gebruik van webscraping om data te verzamelen zodat we met deze data te werk kunnen gaan.

Voor stap 1 maakt men gebruik van 2 scripts: `fetch_data` en `fetch_goals`.

### Voetbalwedstrijdgegevens Scraper (`fetch_data`)

Deze Python-script haalt gegevens op van de website Voetbalkrant om informatie over voetbalwedstrijden in de Belgische Jupiler Pro League van 2002 tot 2023 te extraheren. Het slaat de geëxtraheerde gegevens op in een CSV-bestand met de naam `matches.csv`.

### Vereisten fetch_data

- BeautifulSoup4
- requests

Je kunt de vereiste bibliotheken installeren met pip: pip install beautifulsoup4 requests

### Gebruik fetch_data

1. Kloon of download de repository naar je lokale machine.
2. Open een terminal of opdrachtprompt.
3. Navigeer naar de map met het script.
4. Voer het script uit met Python:
   python fetch_data.py

Het script begint gegevens over wedstrijden op te halen van de website Voetbalkrant en slaat deze op in `matches.csv`. Elke rij in het CSV-bestand vertegenwoordigt een voetbalwedstrijd en bevat de volgende kolommen:

- `ID`: unieke ID per wedstrijd
- `Start Year`: beginjaar seizoen
- `End Year`: eindjaar seizoen
- `Date`: datum van de wedstrijd
- `time`: uur van de wedstrijd
- `Home Team`: naam van de thuisploeg
- `Score`: score van de wedstrijd
- `Away Team`: naam van de uitploeg

### Aanpassing fetch_data

Je kunt het script aanpassen door de volgende variabelen aan te passen:

- `base_url`: De basis-URL van de website Voetbalkrant.
- `start_year`: Het startjaar voor het ophalen van wedstrijdgegevens.
- `end_year`: Het eindjaar voor het ophalen van wedstrijdgegevens.

Standaard haalt het script gegevens op van 2002 tot 2023. Je kunt de variabelen `start_year` en `end_year` aanpassen om gegevens op te halen voor een andere reeks jaren.

### Voetbaldoelpunten Data Scraper (`fetch_goals`)

Dit Python-script is ontwikkeld om informatie over doelpunten in voetbalwedstrijden van de Belgische Jupiler Pro League van 2007 tot 2023 te extraheren van de website `voetbalkrant.com`. Het opgeslagen bestand, `football_goals.csv`, bevat details van elk doelpunt per wedstrijd.

### Vereisten fetch_goals

- BeautifulSoup4
- requests
- csv
- re

Je kunt de vereiste bibliotheken installeren met pip: pip install beautifulsoup4 requests

### Gebruik fetch_goals

1. Kloon of download de repository naar je lokale machine.
2. Open een terminal of opdrachtprompt.
3. Navigeer naar de map met het script.
4. Voer het script uit met Python:
   python fetch_data.py

Het script verzamelt gegevens over doelpunten van de website `voetbalkrant.com` en slaat deze op in `football_goals.csv`. Elke rij in het CSV-bestand vertegenwoordigt een doelpunt in een voetbalwedstrijd en bevat de volgende kolommen:

- `Jaar`: Het jaar van de wedstrijd.
- `Minuut`: De minuut waarin het doelpunt werd gescoord.
- `Speler`: De naam van de speler die het doelpunt heeft gescoord.

### Aanpassing fetch_goals

Je kunt het script aanpassen door de volgende variabelen aan te passen:

- `base_url`: De basis-URL van de website `voetbalkrant.com`.
- `start_year`: Het startjaar voor het ophalen van wedstrijdgegevens.
- `end_year`: Het eindjaar voor het ophalen van wedstrijdgegevens.

Standaard haalt het script gegevens op van 2007 tot 2022. Je kunt de variabelen `start_year` en `end_year` aanpassen om gegevens op te halen voor een andere reeks jaren.
