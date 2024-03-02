import pandas as pd

def control_data(file_path):
    # Load the CSV file
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')

    jaarTallen2puntensysteem = set(range(1960, 1995)) - {1964}

    
    # Initialize a counter for errors
    error_count = 0

    # Iterate over each row in DataFrame
    for index, row in data.iterrows():
        # Check doelpuntensaldo
        if (row['DoelpuntenVoor'] - row['DoelpuntenTegen']) != row['Doelpuntensaldo']:
            print(f"Fout in rij {index}: Doelpuntensaldo klopt niet.")
            error_count += 1
        
        # Determine if season uses 2-point or 3-point system
        if row['SeizoensBegin'] in jaarTallen2puntensysteem:
            # Check puntenVoor and puntenTegen for 2-point system
            if row['AantalGewonnen'] * 2 + row['AantalGelijk'] != row['PuntenVoor']:
                print(f"Fout in rij {index}: PuntenVoor klopt niet met 2-puntensysteem.")
                error_count += 1
            if row['AantalVerloren'] * 2 + row['AantalGelijk'] != row['PuntenTegen']:
                print(f"Fout in rij {index}: PuntenTegen klopt niet met 2-puntensysteem.")
                error_count += 1
        else:
            # Check puntenVoor and puntenTegen for 3-point system
            if row['AantalGewonnen'] * 3 + row['AantalGelijk'] != row['PuntenVoor']:
                print(f"Fout in rij {index}: PuntenVoor klopt niet met 3-puntensysteem.")
                error_count += 1
            if row['AantalVerloren'] * 3 + row['AantalGelijk'] != row['PuntenTegen']:
                print(f"Fout in rij {index}: PuntenTegen klopt niet met 3-puntensysteem.")
                error_count += 1

    if error_count == 0:
        print("Alle checks zijn succesvol. Geen fouten gevonden.")
    else:
        print(f"Aantal fouten gevonden: {error_count}")

    return data


# Path to your cleaned CSV file
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
controlled_data = control_data(file_path)

# Save the controlled data to a new CSV
controlled_data.to_csv('stand_controlled.csv', index=False)