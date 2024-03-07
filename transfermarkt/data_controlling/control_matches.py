import pandas as pd

def remove_records_with_more_matches_than_matchdays(data):
    # Get the number of unique matchdays
    num_matchdays = data['Speeldag'].nunique()
    
    # Get the total number of matches
    num_matches = len(data)
    
    # Check if there are records with more matches than matchdays
    if num_matches > num_matchdays:
        print("Er zijn records met meer wedstrijden dan speeldagen. Deze worden verwijderd.")
        
        # Filter records with more matches than matchdays
        records_with_more_matches = data[data['Speeldag'] > num_matches / num_matchdays]
        
        # Remove records with more matches than matchdays
        data = data.drop(records_with_more_matches.index)
        
        num_deleted_records = len(records_with_more_matches)
        
        if num_deleted_records == 0:
            print("Er zijn geen records verwijderd. Data is OK.")
        else:
            print(f"{num_deleted_records} records zijn verwijderd.")
    else:
        print("Data is OK, er zijn geen records met meer wedstrijden dan speeldagen.")
    
    return data

# Example usage
file_path = r'C:\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\matches_clean.csv'
cleaned_data = pd.read_csv(file_path, encoding='utf-8')
cleaned_data = remove_records_with_more_matches_than_matchdays(cleaned_data)

if cleaned_data is not None:
    # Save the controlled data to a new CSV
    cleaned_data.to_csv(r'C:\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\matches_controlled.csv', index=False)
