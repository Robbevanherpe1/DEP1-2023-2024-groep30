import pandas as pd
import re

# Laden van het bestand "matches.csv"
df = pd.read_csv(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\scraped_data\matches.csv')

# Aanpassen van de kolomnamen
df.columns = ['Match_ID', 'Seizoen', 'Speeldag', 'Datum', 'Tijdstip', 'Thuisploeg', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg', 'Uitploeg']

# Functie om de datum te parsen en om te zetten naar het juiste formaat
def parse_date(date_str):
    parts = date_str.split(',')
    if len(parts) >= 2:  # Controleren of de lijst voldoende elementen heeft
        day_month_year = parts[1].split()  # Verdelen van de dag, maand en jaar
        day = day_month_year[0][2:]  # Dag uit 'zo4'
        month_str = day_month_year[1]  # Maand uit 'sep.'
        # Maanden afkorten naar hun volledige naam
        months = {
            'jan.': '01',
            'feb.': '02',
            'mrt.': '03',
            'apr.': '04',
            'mei': '05',
            'jun.': '06',
            'jul.': '07',
            'aug.': '08',
            'sep.': '09',
            'okt.': '10',
            'nov.': '11',
            'dec.': '12'
        }
        month = months.get(month_str.lower(), month_str)  # Controleren of de afkorting in de dictionary voorkomt
        year = day_month_year[2]  # Jaar
        return f"{year}/{month}/{day}"
    else:
        return date_str  # Als het formaat niet kan worden geparseerd, retourneren we de originele waarde

# Functie om het tijdstip te formatteren
def parse_time(time_str):
    return time_str.split()[0]  # Het tijdstip is het eerste deel van de string

# Functie om het resultaat op te schonen en alleen de score te behouden
def clean_result(result_str):
    if pd.isna(result_str):  # Controleren of de waarde NaN is
        return result_str
    else:
        parts = str(result_str).split(',')  # Converteer eerst naar een string om te vermijden dat een AttributeError optreedt
        if len(parts) > 1:  # Controleren of de string een komma bevat
            return int(float(parts[0]))  # Converteer naar float en vervolgens naar geheel getal
        else:
            return result_str  # Als er geen komma is, retourneren we de originele waarde

# Functie om de naam van het team op te schonen
def clean_team(team_str):
    return re.sub(r'\([^)]*\)', '', team_str).strip()

# Omzetten van de datum naar het juiste formaat
df['Datum'] = df['Datum'].apply(parse_date)

# Omzetten van het tijdstip naar het juiste formaat
df['Tijdstip'] = df['Tijdstip'].apply(parse_time)

# Opschonen van de resultaatkolommen en converteren naar gehele getallen
df['Resultaat_Thuisploeg'] = df['Resultaat_Thuisploeg'].apply(clean_result)
df['Resultaat_Uitploeg'] = df['Resultaat_Uitploeg'].apply(clean_result)

# Verwijderen van NaN-waarden in de resultaatkolommen
df.dropna(subset=['Resultaat_Thuisploeg', 'Resultaat_Uitploeg'], inplace=True)

# Converteren naar gehele getallen
df['Resultaat_Thuisploeg'] = df['Resultaat_Thuisploeg'].astype(int)
df['Resultaat_Uitploeg'] = df['Resultaat_Uitploeg'].astype(int)

# Opschonen van de teamnamen
df['Thuisploeg'] = df['Thuisploeg'].apply(clean_team)
df['Uitploeg'] = df['Uitploeg'].apply(clean_team)

# Opslaan van de aangepaste gegevens naar "clean_matches.csv"
df.to_csv(r'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\matches_clean.csv', index=False, header=True)
