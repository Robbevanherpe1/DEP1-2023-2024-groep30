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
    
    # Pas kolomnamen in theoretische_speeldagen aan naar hoofdletters
    theoretische_speeldagen.columns = [col.capitalize() for col in theoretische_speeldagen.columns]

    # Verwijder kolommen uit wedstrijden die ook in theoretische_speeldagen voorkomen (indien aanwezig)
    for col in ['Seizoen', 'Speeldag']:
        if col in wedstrijden.columns:
            wedstrijden.drop(col, axis=1, inplace=True)

    # Zet de datums om naar hetzelfde formaat
    theoretische_speeldagen['Datum'] = pd.to_datetime(theoretische_speeldagen['Datum'], format='%Y/%m/%d')
    wedstrijden['Datum'] = pd.to_datetime(wedstrijden['Datum'], format='%Y-%m-%d')

    # Sorteer theoretische speeldagen op datum voor efficiënte zoekopdrachten
    theoretische_speeldagen = theoretische_speeldagen.sort_values(by='Datum')
    
    # Merge logica: gebruik 'forward' om de speeldag en seizoen van de eerstvolgende datum in theoretische_speeldagen toe te wijzen
    merged_data = pd.merge_asof(wedstrijden.sort_values('Datum'), theoretische_speeldagen,
                                on='Datum', direction='forward')

    return merged_data

# Bestandspaden voor csv-bestanden
file_path_theoretisch_speeldagen = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data_merge\theoretische_speeldagen.csv'
file_path_wedstrijden = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data_merge\wedstrijden.csv'
file_path_merged_data = r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\wedstrijden.csv'

# Uitvoeren van de functie
merged_data = merge_data(file_path_theoretisch_speeldagen, file_path_wedstrijden)

# Opslaan van gecontroleerde data en errors
merged_data.to_csv(file_path_merged_data, index=False, header=True, sep=';')
