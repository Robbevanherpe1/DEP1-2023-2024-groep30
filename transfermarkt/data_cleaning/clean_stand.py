import pandas as pd
import os

csv_path = '../data/stand.csv'

if os.path.exists(csv_path):
    stand = pd.read_csv(csv_path)
    print(stand.head(10))
else:
    print(f"File not found: {csv_path}")
