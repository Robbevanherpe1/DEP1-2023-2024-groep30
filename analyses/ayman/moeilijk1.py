import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import norm

# Load data
data = pd.read_csv('clean_matches.csv', sep=';')

# Convert 'Seizoen' column to a more readable format
data['Seizoen'] = data['Seizoen'].str.replace('/', '-')

# Calculate total goals per match
data['TotalGoals'] = data['FinaleStandThuisploeg'] + data['FinaleStandUitploeg']

# Calculate average goals per match per season
seasonal_averages = data.groupby('Seizoen')['TotalGoals'].mean()

# Plot line graph
plt.figure(figsize=(10,6))
plt.plot(seasonal_averages.index, seasonal_averages.values, label='Average Goals per Match', marker='o')
plt.xlabel('Season')
plt.ylabel('Average Goals per Match')
plt.title('Average Goals per Match per Season')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)

# Show data from 1960 to last season
first_season = seasonal_averages.index[0]
last_season = seasonal_averages.index[-1]
plt.xlim(left=first_season, right=last_season)

plt.show()

# Calculate overall averages
historical_data = data[data['Seizoen'] <= '2016-2017']
recent_data = data[data['Seizoen'] >= '2017-2018']

mean_historical = historical_data['TotalGoals'].mean()
mean_recent = recent_data['TotalGoals'].mean()

# Given values
std_dev = historical_data['TotalGoals'].std()
alpha = 0.05         # level of significance
sample_size = len(recent_data)      # number of matches

# Region of Rejection
z_critical = norm.ppf(1 - alpha)
rejection_boundary = mean_historical + z_critical * (std_dev / (sample_size ** 0.5))

# Probability of Type I Error
prob_type_I_error = alpha

# Determine if there is a significant difference
if mean_recent > rejection_boundary:
    print("There is a significant increase in the average number of goals per match in recent years.")
elif mean_recent < mean_historical:
    print("There is a significant decrease in the average number of goals per match in recent years.")
else:
    print("There is no significant difference in the average number of goals per match between the historical and recent data.")

print(f"Average goals per match (historical): {mean_historical:.3f}")
print(f"Average goals per match (recent): {mean_recent:.3f}")
print(f"Region of Rejection: Average goals per match > {rejection_boundary:.3f}")
print(f"Probability of Type I Error: {prob_type_I_error * 100}%")

# Add more recent seasons
more_recent_data = data[data['Seizoen'] >= '2020-2021']
mean_more_recent = more_recent_data['TotalGoals'].mean()

print(f"Average goals per match (more recent): {mean_more_recent:.3f}")