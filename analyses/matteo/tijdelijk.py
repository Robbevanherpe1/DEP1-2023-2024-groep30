def verwerk_en_categoriseer_wedstrijden(wedstrijden, klassement):
    # Zet 'Seizoen' en 'Speeldag' in beide dataframes om naar strings voor consistentie
    wedstrijden['Seizoen'] = wedstrijden['Seizoen'].astype(str)
    klassement['Seizoen'] = klassement['Seizoen'].astype(str)
    wedstrijden['Speeldag'] = wedstrijden['Speeldag'].astype(int)
    klassement['Speeldag'] = klassement['Speeldag'].astype(int)

    # Voeg de stand van de voorgaande speeldag toe aan het klassement
    klassement_vorige_speeldag = klassement.copy()
    klassement_vorige_speeldag['Speeldag'] += 1

    # Merge op 'Seizoen', 'Speeldag', en 'Stamnummer' voor thuisploeg
    wedstrijden = pd.merge(wedstrijden, klassement_vorige_speeldag[['Seizoen', 'Speeldag', 'Stamnummer', 'Stand']], left_on=['Seizoen', 'Speeldag', 'StamnummerThuisploeg'], right_on=['Seizoen', 'Speeldag', 'Stamnummer'], how='left')
    wedstrijden.rename(columns={'Stand': 'PositieThuis'}, inplace=True)

    # Merge op 'Seizoen', 'Speeldag', en 'Stamnummer' voor uitploeg
    wedstrijden = pd.merge(wedstrijden, klassement_vorige_speeldag[['Seizoen', 'Speeldag', 'Stamnummer', 'Stand']], left_on=['Seizoen', 'Speeldag', 'StamnummerUitploeg'], right_on=['Seizoen', 'Speeldag', 'Stamnummer'], how='left')
    wedstrijden.rename(columns={'Stand': 'PositieUit'}, inplace=True)

    # Bepaal segmenten voor thuis en uit teams
    totaal_teams = klassement['Stamnummer'].nunique()
    wedstrijden['SegmentThuis'] = wedstrijden['PositieThuis'].apply(lambda x: segment_bepalen(x, totaal_teams))
    wedstrijden['SegmentUit'] = wedstrijden['PositieUit'].apply(lambda x: segment_bepalen(x, totaal_teams))

    # Categoriseer wedstrijden gebaseerd op segmenten
    conditions = [
        (wedstrijden['SegmentThuis'] == wedstrijden['SegmentUit']),
        ((wedstrijden['SegmentThuis'] == 1) | (wedstrijden['SegmentUit'] == 1)) & ((wedstrijden['SegmentThuis'] == 3) | (wedstrijden['SegmentUit'] == 3)),
        ((wedstrijden['SegmentThuis'] == 1) & (wedstrijden['SegmentUit'] == 2)) | ((wedstrijden['SegmentThuis'] == 2) & (wedstrijden['SegmentUit'] == 1))
    ]
    choices = ['dicht', 'gemiddeld', 'ver']
    wedstrijden['Categorie'] = np.select(conditions, choices, default='niet-geclassificeerd')

    return wedstrijden

# Functie om segmenten te bepalen op basis van posities
def segment_bepalen(positie, totaal_teams):
    if positie <= 6:
        return 1  # Top 6 teams
    elif positie > totaal_teams - 6:
        return 2  # Laatste 6 teams
    else:
        return 3  # Midden teams

def statistieken_berekenen_en_toevoegen(wedstrijden):
    # Bereken het totaal aantal doelpunten per wedstrijd
    wedstrijden['TotaalDoelpunten'] = wedstrijden['StandThuis'] + wedstrijden['StandUit']

    # Bereken het totaal aantal wedstrijden en doelpunten per categorie
    wedstrijden['TotaalWedstrijdenPerCategorie'] = wedstrijden.groupby('Categorie')['Id'].transform('count')
    wedstrijden['TotaalDoelpuntenPerCategorie'] = wedstrijden.groupby('Categorie')['TotaalDoelpunten'].transform('sum')

    return wedstrijden

def goodness_of_fit_test(wedstrijden):
    categories = ['dicht', 'gemiddeld', 'ver']
    observed = []
    
    for category in categories:
        if category in wedstrijden.index:
            goals = wedstrijden.loc[category, 'TotaalDoelpunten']
            non_goals = wedstrijden.loc[category, 'TotaalWedstrijden'] * 2 - goals  # Vermenigvuldig met 2 voor thuis en uit
            if non_goals >= 0 and (goals + non_goals > 5):  # Zorg dat er genoeg gegevens zijn voor de test
                observed.append([goals, non_goals])
            else:
                return {'error': f'Niet genoeg gegevens voor categorie {category}'}
        else:
            return {'error': f'Categorie {category} ontbreekt in de data'}

    if len(observed) > 1:
        chi2, p, dof, expected = chi2_contingency(observed)
        return {'chi2_statistic': chi2, 'p_value': p, 'degrees_of_freedom': dof, 'expected_frequencies': expected}
    else:
        return {'error': 'Niet genoeg categorieën met voldoende gegevens voor een geldige chi-kwadraattoets'}


# Voorbeeldgebruik
wedstrijden = verwerk_en_categoriseer_wedstrijden(wedstrijdendata, klassementdata)
wedstrijden = statistieken_berekenen_en_toevoegen(wedstrijden)
test_resultaten = goodness_of_fit_test(wedstrijden)

# Print de resultaten
print(test_resultaten)
