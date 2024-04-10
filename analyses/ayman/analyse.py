import pandas as pd

def analyseer_duels_data_science(club1, club2, csv_bestand):

    
    # Lees de CSV-gegevens in een DataFrame
    df = pd.read_csv(csv_bestand)
    
    # Filter de gegevens op de clubs van interesse
    df_filtered = df[(df['StamnummerThuis'] == club1) | (df['StamnummerUit'] == club1) |
                     (df['StamnummerThuis'] == club2) | (df['StamnummerUit'] == club2)]
    
    # Bereken de uitslagen
    df_filtered['uitslag'] = df_filtered.apply(lambda row: 'gewonnen' if row['StamnummerThuis'] == club1 and row['B365ThuisWint'] else
                                                          'gewonnen' if row['StamnummerUit'] == club1 and row['B365UitWint'] else
                                                          'gelijk' if row['B365Gelijkspel'] else 'verloren', axis=1)
    
    # Bereken de tellers en percentages
    uitslagen = df_filtered['uitslag'].value_counts(normalize=True) * 100
    
    # Bouw de resultaatstring
    resultaat = f"{club1} heeft {uitslagen.get('gewonnen', 0):.2f}% gewonnen, {club2} heeft {uitslagen.get('gewonnen', 0):.2f}% gewonnen, en er zijn {uitslagen.get('gelijk', 0):.2f}% gelijkspel."
    
    return resultaat