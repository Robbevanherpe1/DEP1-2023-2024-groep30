import pandas as pd
import math

def bereken_klassement(csv_file):
    # Lees de CSV-gegevens in een DataFrame
    df = pd.read_csv(csv_file, sep=',', header=0)
    
    # Filter de gegevens voor Playoff 1
    df_playoff1 = df[df['type'] == 'Playoff_1']
    
    # Bereken het klassement
    # Begin met de punten van de laatste speeldag in de reguliere competitie, deel door 2 (afgerond naar boven)
    df_playoff1['start_punten'] = (df_playoff1['stand_thuis'] + df_playoff1['stand_uit']) // 2
    
    # Voeg de punten van Playoff 1 toe aan de startpunten
    df_playoff1['totaal_punten'] = df_playoff1['start_punten'] + df_playoff1['stand_thuis'] * 3 + df_playoff1['stand_uit'] * 3
    
    # Sorteer op totaalpunten
    df_playoff1 = df_playoff1.sort_values(by='totaal_punten', ascending=False)
    
    # Geef het klassement weer
    print("Klassement voor Playoff 1:")
    print(df_playoff1[['tf_clubnaam_thuis', 'totaal_punten']])
    print(df_playoff1[['tf_clubnaam_uit', 'totaal_punten']])

# Voorbeeldgebruik
csv_file = "wedstrijd_playoffs.csv"
bereken_klassement(csv_file)
