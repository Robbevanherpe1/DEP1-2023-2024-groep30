#!/usr/bin/env python3

import csv

input_filename = '/home/vicuser/data/bets.csv'
output_filename = '/home/vicuser/data/betsCorrect.csv'

# Lees de data en sla de unieke regels op, met uitzondering van de timestamp
unique_rows = set()
with open(input_filename, 'r', newline='') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # Sla de kopregel op
    for row in reader:
        # Negeer de timestamp (laatste kolom) voor uniciteit check
        unique_key = tuple(row[:-1])
        unique_rows.add(unique_key)

# Schrijf de unieke regels naar het nieuwe bestand
with open(output_filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)  # Schrijf de kopregel
    for row in sorted(unique_rows, key=lambda x: (x[0], x[2])):  # Sorteer op ID en Starttijd
        writer.writerow(row + ('',))  # Voeg een lege timestamp toe

print(f'{len(unique_rows)} unieke regels zijn geschreven naar {output_filename}.')