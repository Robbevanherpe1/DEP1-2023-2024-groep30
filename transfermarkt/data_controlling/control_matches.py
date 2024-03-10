import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder

def controleerdatatypes_ml(file_path):
    # Het inlezen van de dataset
    data = pd.read_csv(file_path)

    # Gewenste datatypes
    gewenste_dtypes = {
        'Seizoen': 'object',
        'Speeldag': 'int64',
        'Resultaat_Thuisploeg': 'int64',
        'Resultaat_Uitploeg': 'int64',
        'Thuisploeg_stamnummer': 'int64',
        'Uitploeg_stamnummer': 'int64'
    }

    # Controleren van huidige datatypes en vergelijken met gewenste datatypes
    verschillen = []
    for kolom in data.columns:
        if kolom in gewenste_dtypes and data.dtypes[kolom] != gewenste_dtypes[kolom]:
            verschillen.append(kolom)
    
    if verschillen:
        for kolom in verschillen:
            # Probeer te converteren naar het gewenste datatype
            try:
                data[kolom] = pd.to_numeric(data[kolom], errors='coerce')
                data[kolom] = data[kolom].astype(gewenste_dtypes[kolom])
            except ValueError:
                print(f"Kon kolom '{kolom}' niet converteren naar het gewenste datatype '{gewenste_dtypes[kolom]}'.")
                return None
        print("Alle datatypes zijn aangepast naar de gewenste datatypes.")
        return data
    else:
        return data

def remove_records_with_more_matches_than_matchdays(data):
    # Get the number of unique matchdays
    num_matchdays = data['Seizoen'].nunique()
    
    # Get the total number of matches
    num_matches = len(data)
    
    # Selecteer alleen numerieke kolommen
    numeric_columns = ['Seizoen', 'Speeldag', 'Resultaat_Thuisploeg', 'Resultaat_Uitploeg', 
                       'Thuisploeg_stamnummer', 'Uitploeg_stamnummer']
    
    features = data[numeric_columns]
    
    # Fit Isolation Forest model
    model = IsolationForest(contamination='auto', random_state=42)
    model.fit(features)
    
    # Voorspel outliers
    outliers = model.predict(features)
    
    # Filter records with more matches than matchdays
    records_with_more_matches = data[outliers == 1]  # 1 represents inliers
    
    # Verwijder records met meer wedstrijden dan matchdagen
    data = records_with_more_matches
    
    num_deleted_records = len(data)
        
    if num_deleted_records == num_matches:
        print("Er zijn geen records verwijderd. Data is OK.")
    else:
        print(f"{num_deleted_records} records zijn verwijderd.")
    
    return data

# Voorbeeldgebruik
file_path = r'C:\Users\ayman\OneDrive\Bureaublad\Backup\clean_matches.csv'
cleaned_data = controleerdatatypes_ml(file_path)

if cleaned_data is not None:
    cleaned_data = remove_records_with_more_matches_than_matchdays(cleaned_data)

    if cleaned_data is not None:
        # Opslaan van de gecontroleerde gegevens naar een nieuwe CSV
        cleaned_data.to_csv(r'C:\Users\ayman\OneDrive\Bureaublad\Backup\matches_controlled.csv', index=False)
