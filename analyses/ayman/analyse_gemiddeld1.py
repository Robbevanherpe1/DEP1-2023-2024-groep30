import csv

def lees_wedstrijden(bestandsnaam, delimiter):
    """Leest de wedstrijden uit een CSV-bestand en retourneert een lijst met wedstrijden."""
    wedstrijden = []
    with open(bestandsnaam, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        for row in reader:
            wedstrijden.append(row)
    return wedstrijden

def bereken_punten(wedstrijden, seizoen):
    """Berekent de punten voor elke ploeg op basis van de wedstrijden voor een specifiek seizoen."""
    punten = {}
    for wedstrijd in wedstrijden:
        if wedstrijd['Seizoen'] != seizoen:
            continue # Sla deze wedstrijd over als het niet overeenkomt met het opgegeven seizoen
        
        thuisploeg = wedstrijd['RoepnaamThuisploeg']
        uitploeg = wedstrijd['RoepnaamUitploeg']
        finale_stand_thuis = float(wedstrijd['FinaleStandThuisploeg'])
        finale_stand_uit = float(wedstrijd['FinaleStandUitploeg'])
        
        if thuisploeg not in punten:
            punten[thuisploeg] = 0
        if uitploeg not in punten:
            punten[uitploeg] = 0
        
        if finale_stand_thuis > finale_stand_uit:
            punten[thuisploeg] += 3
        elif finale_stand_thuis == finale_stand_uit:
            punten[thuisploeg] += 1
            punten[uitploeg] += 1
        else:
            punten[uitploeg] += 3
    
    return punten


def bereken_playoff_punten(punten, playoffs, seizoen):
    """Berekent de extra punten voor elke ploeg op basis van de Play-off wedstrijden."""
    extra_punten = {}
    for playoff in playoffs:
        if playoff['seizoen_alternatief'] != seizoen or playoff['type'] != 'Playoff_1':
            continue # Sla deze wedstrijd over als het niet overeenkomt met het opgegeven seizoen of als het niet Play-off 1 is
    
    for playoff in playoffs:
        thuisploeg = playoff['tf_clubnaam_thuis']
        uitploeg = playoff['tf_clubnaam_uit']
        stand_thuis = int(playoff['stand_thuis'])
        stand_uit = int(playoff['stand_uit'])
        
        if thuisploeg not in extra_punten:
            extra_punten[thuisploeg] = 0
        if uitploeg not in extra_punten:
            extra_punten[uitploeg] = 0
        
        if stand_thuis > stand_uit:
            extra_punten[thuisploeg] += 3
        elif stand_thuis == stand_uit:
            extra_punten[thuisploeg] += 1
            extra_punten[uitploeg] += 1
        else:
            extra_punten[uitploeg] += 3
    
    return extra_punten

def toon_resultaat(punten):
    """Toont de berekende punten voor elke ploeg."""
    for ploeg, punten in punten.items():
        print(f"Ploeg {ploeg}: {punten} punten")

# Lees de wedstrijden uit het CSV-bestand met puntkomma als scheidingsteken
wedstrijden = lees_wedstrijden('clean_matches.csv', ';')

# Bereken de punten voor elke ploeg voor het seizoen 21/22
punten = bereken_punten(wedstrijden, "21/22")

# Toon de resultaten
toon_resultaat(punten)
