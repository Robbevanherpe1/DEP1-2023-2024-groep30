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

Voor stap 1 maakt men gebruik van 3 scripts: `fetch_stand`, `fetch_matches` en `fetch_goals`.

### Voetbalwedstrijdgegevens Scraper (`fetch_matches`)

Deze Python-script haalt gegevens op van de website Transfermarkt om informatie over voetbalwedstrijden in de Belgische Jupiler Pro League van 1960 tot de huidige datum te extraheren. Het slaat de geëxtraheerde gegevens op in een CSV-bestand met de naam `matches.csv`.

### Vereisten fetch_matches

- BeautifulSoup4
- requests
- csv
- datetime

Je kunt de vereiste bibliotheken installeren met pip: pip install beautifulsoup4 requests csv datetime

### Gebruik fetch_matches

1. Kloon of download de repository naar je lokale machine.
2. Open een terminal of opdrachtprompt.
3. Navigeer naar de map met het script.
4. Voer het script uit met Python:
   python fetch_matches.py

Het script begint gegevens over wedstrijden op te halen van de website transfermarkt en slaat deze op in `matches.csv`. Elke rij in het CSV-bestand vertegenwoordigt een voetbalwedstrijd en bevat de volgende kolommen:

- `Match_ID`: unieke ID per wedstrijd
- `Seizoen`: beginjaar en eindjaar seizoen
- `Speeldag`: de speeldag
- `Datum`: datum van de wedstrijd
- `Tijdstip`: uur van de wedstrijd
- `Thuisploeg`: naam van de thuisploeg
- `Resultaat_Thuisploeg`: score van de thuisploeg
- `Resultaat_Uitploeg`: score van de uitploeg
- `Uitploeg`: naam van de uitploeg

### Voetbalwedstrijdstand Scraper (`fetch_stand`)

Deze Python-script haalt gegevens op van de website Transfermarkt om informatie over de stand in de Belgische Jupiler Pro League van 1960 tot de huidige datum te extraheren. Het slaat de geëxtraheerde gegevens op in een CSV-bestand met de naam `stand.csv`.

### Vereisten fetch_stand

- BeautifulSoup4
- requests
- csv
- datetime

Je kunt de vereiste bibliotheken installeren met pip: pip install beautifulsoup4 requests csv datetime

### Gebruik fetch_stand

1. Kloon of download de repository naar je lokale machine.
2. Open een terminal of opdrachtprompt.
3. Navigeer naar de map met het script.
4. Voer het script uit met Python:
   python fetch_stand.py

Het script begint gegevens over de stand op te halen van de website transfermarkt en slaat deze op in `stand.csv`. Elke rij in het CSV-bestand vertegenwoordigt een voetbalwedstrijd en bevat de volgende kolommen:

- `Seizoen`: beginjaar en eindjaar seizoen
- `Speeldag`: de speeldag
- `Stand`: plaats in stand
- `Club`: naam van de club
- `AantalGespeeld`: aantal matchen gespeeld
- `AantalGewonnen`: aantal matchen gewonnen
- `AantalGelijk`: aantal matchen gelijk gespeeld
- `AantalVerloren`: aantal matchen verloren
- `Doelpunten`: aantal doelpunten
- `Doelpuntensaldo`: het doelpunten saldo
- `Punten`: aantal punten

### Voetbaldoelpunten Data Scraper (`fetch_goals`)

Dit Python-script is ontwikkeld om informatie over doelpunten in Transfermarkt van de Belgische Jupiler Pro League van 2007 tot de huidige datum te extraheren van de website `Transfermarkt.com`. Het opgeslagen bestand, `goals.csv`, bevat details van elk doelpunt per wedstrijd.

### Vereisten fetch_goals

- BeautifulSoup4
- datetime
- requests
- csv
- re

Je kunt de vereiste bibliotheken installeren met pip: pip install beautifulsoup4 requests datetime

### Gebruik fetch_goals

1. Kloon of download de repository naar je lokale machine.
2. Open een terminal of opdrachtprompt.
3. Navigeer naar de map met het script.
4. Voer het script uit met Python:
   python fetch_goals.py

Het script verzamelt gegevens over doelpunten van de website `Transfermarkt.com` en slaat deze op in `goals.csv`. Elke rij in het CSV-bestand vertegenwoordigt een doelpunt in een voetbalwedstrijd en bevat de volgende kolommen:

- `Match_ID`: unieke ID per wedstrijd
- `Seizoen`: beginjaar en eindjaar seizoen
- `Speeldag`: de speeldag
- `Datum`: datum van de wedstrijd
- `Tijdstip`: uur van de wedstrijd
- `Thuisploeg`: naam van de thuisploeg
- `Uitploeg`: naam van de uitploeg
- `NaamScorendePloeg`: naam van de ploeg die scoort
- `GoalTijdstip`: tijdstip goal
- `GoalScorer`: naam van goal scorer
- `StandThuisploeg`: stand van thuisploeg
- `StandUitploeg`: stand van uitploeg
