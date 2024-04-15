import pandas as pd
import matplotlib.pyplot as plt

# Laad de data
data = pd.read_csv('clean_matches.csv', sep=';', parse_dates=['Datum'], dayfirst=True)

# Filter de data voor de laatste 7 seizoenen en de historische data
data_filtered = data[(data['Seizoen'] >= '2017/22') | (data['Seizoen'] <= '2016/21')]

# Bereken het totaal aantal doelpunten per match voor elk seizoen
totaal_doelpunten_per_match = data_filtered.groupby('Seizoen')[['FinaleStandThuisploeg', 'FinaleStandUitploeg']].sum().sum(axis=1)

# Bereken het aantal wedstrijden per seizoen
aantal_wedstrijden_per_seizoen = data_filtered.groupby('Seizoen').size()

# Bereken het gemiddelde aantal doelpunten per match per seizoen
gemiddelde_doelpunten = totaal_doelpunten_per_match / aantal_wedstrijden_per_seizoen

# Print het resultaat
print(gemiddelde_doelpunten)

# Maak een lijngrafiek van het gemiddelde aantal doelpunten per match per seizoen
plt.plot(gemiddelde_doelpunten.index, gemiddelde_doelpunten.values)
plt.xlabel('Seizoen')
plt.ylabel('Gemiddeld aantal doelpunten per match')
plt.title('Gemiddeld aantal doelpunten per match per seizoen')
plt.show()

# Voorbeeld van het toevoegen van fictieve classificatiegegevens
# Let op: Dit is een vereenvoudigd voorbeeld en vereist aanpassing aan je specifieke data
data_filtered['thuis_klassement'] = data_filtered['RoepnaamThuisploeg'].apply(lambda x: 1 if x == 'Sint-Truidense VV' else 2)
data_filtered['uit_klassement'] = data_filtered['RoepnaamUitploeg'].apply(lambda x: 1 if x == 'Standard Luik' else 2)

# Bereken de categorie voor elke wedstrijd
data_filtered['categorie'] = data_filtered.apply(lambda row: 'dicht' if abs(row['thuis_klassement'] - row['uit_klassement']) <= 6 else 'ver', axis=1)

# Bereken het totaal aantal wedstrijden en doelpunten per categorie
totaal_wedstrijden_per_categorie = data_filtered.groupby('categorie')['Seizoen'].count()
totaal_doelpunten_per_categorie = data_filtered.groupby('categorie')[['FinaleStandThuisploeg', 'FinaleStandUitploeg']].sum().sum(axis=1) / (2 * len(data_filtered))

# Print de resultaten
print("Totaal aantal wedstrijden per categorie:")
print(totaal_wedstrijden_per_categorie)
print("\nTotaal aantal doelpunten per categorie:")
print(totaal_doelpunten_per_categorie)
