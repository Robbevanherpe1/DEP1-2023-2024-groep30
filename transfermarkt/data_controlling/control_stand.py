import pandas as pd

def control_data(file_path):
    # Load the CSV file
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')

    # Convert specified columns to integer
    int_columns = ['SeizoensBegin', 'SeizoensEinde', 'AantalGewonnen', 'AantalGelijk', 'AantalVerloren',
                   'DoelpuntenVoor', 'DoelpuntenTegen', 'Doelpuntensaldo', 'PuntenVoor', 'PuntenTegen']
    for column in int_columns:
        data[column] = data[column].astype('Int64')  # Note the capital "I" in 'Int64'

    # Check doelpuntensaldo
    data['DoelpuntensaldoCheck'] = (data['DoelpuntenVoor'] - data['DoelpuntenTegen']) == data['Doelpuntensaldo']

    # Apply checks for PuntenVoor and PuntenTegen based on SeizoensBegin
    # Note: Instead of using if statements, use vectorized operations
    data['PuntenVoorCheck'] = False  # Initialize the column with default False
    data['PuntenTegenCheck'] = False  # Initialize the column with default False

    # For seasons before 1995
    before_1995 = data['SeizoensBegin'] < 1995
    data.loc[before_1995, 'PuntenVoorCheck'] = (data['AantalGewonnen'] * 2 + data['AantalGelijk'] == data['PuntenVoor']) & before_1995
    data.loc[before_1995, 'PuntenTegenCheck'] = (data['AantalVerloren'] * 2 + data['AantalGelijk'] == data['PuntenTegen']) & before_1995

    # For seasons from 1995 onwards
    from_1995 = data['SeizoensBegin'] >= 1995
    data.loc[from_1995, 'PuntenVoorCheck'] = (data['AantalGewonnen'] * 3 + data['AantalGelijk'] == data['PuntenVoor']) & from_1995
    data.loc[from_1995, 'PuntenTegenCheck'] = (data['AantalVerloren'] * 3 + data['AantalGelijk'] == data['PuntenTegen']) & from_1995

    # Identifying rows with errors
    incorrect_rows = data[~data['DoelpuntensaldoCheck'] | ~data['PuntenVoorCheck'] | ~data['PuntenTegenCheck']]
    if not incorrect_rows.empty:
        print("Incorrect rows found:")
        for index, row in incorrect_rows.iterrows():
            errors = []
            if not row['DoelpuntensaldoCheck']:
                errors.append("Incorrect 'Doelpuntensaldo'")
            if not row['PuntenVoorCheck']:
                errors.append("Incorrect 'PuntenVoor'")
            if not row['PuntenTegenCheck']:
                errors.append("Incorrect 'PuntenTegen'")
            print(f"Row {index}: {row.to_dict()}, Errors: {', '.join(errors)}")

    return data

# Path to your cleaned CSV file
file_path = r'D:\Hogent\Visual Studio Code\DEP\DEP-G30\DEP1-2023-2024-groep30\transfermarkt\data\cleaned_data\stand_clean.csv'
controlled_data = control_data(file_path)

# Save the controlled data to a new CSV
controlled_data.to_csv('stand_controlled.csv', index=False)