import pandas as pd

# Functie om gegevens te laden uit een CSV-bestand met gespecificeerde coderingen
def load_data(file_path, encoding_list=['utf-8', 'ISO-8859-1']):
    for encoding in encoding_list:
        try:
            return pd.read_csv(file_path, encoding=encoding, sep=';')
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Kon het bestand {file_path} niet laden met de opgegeven coderingen.")

def merge_data(file_path_theoretisch_speeldagen, file_path_wedstrijden):
    # Laden van data
    theoretische_speeldagen = load_data(file_path_theoretisch_speeldagen)
    wedstrijden = load_data(file_path_wedstrijden)

    # Zet de datums om naar hetzelfde formaat
    theoretische_speeldagen['datum'] = pd.to_datetime(theoretische_speeldagen['datum'])
    wedstrijden['Datum'] = pd.to_datetime(wedstrijden['Datum'])

    # Sorteer theoretische speeldagen op datum voor efficiënte zoekopdrachten
    theoretische_speeldagen = theoretische_speeldagen.sort_values(by='datum')
    
    # Merge logica: voeg 'seizoen' en 'speeldag' toe aan wedstrijden
    merged_data = pd.merge_asof(wedstrijden.sort_values('Datum'), theoretische_speeldagen, left_on='Datum', right_on='datum', direction='backward')

    return merged_data


# Bestandspaden voor csv-bestanden
file_path_theoretisch_speeldagen = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data_merge\theoretische_speeldagen.csv'
file_path_wedstrijden = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data_merge\wedstrijden.csv'
file_path_merged_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\wedstrijden2.csv'

# Uitvoeren van de functie
merged_data = merge_data(file_path_theoretisch_speeldagen, file_path_wedstrijden)

# Opslaan van gecontroleerde data en errors
merged_data.to_csv(file_path_merged_data, index=False, header=False, sep=';')
