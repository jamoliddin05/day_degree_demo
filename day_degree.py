from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def adjust_gens(i, gens, avg_humidity, pest_humidity):
    if i == 0:
        return gens[0]
    else:
        alpha = np.exp(-((avg_humidity[i] - pest_humidity) / 20) ** 2)
        return (gens[i] / gens[i - 1]) * adjust_gens(i - 1, gens, avg_humidity, pest_humidity) * (.2*alpha + 0.8)


# Define the database connection parameters
username = "test"
password = "test123"
host = "dev-oxus-backend.amudar.io"  # e.g., "localhost" or "127.0.0.1"
port = "3306"  # default MySQL port
database = "diseaseModels"

connection_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_url, connect_args={"ssl": {"ssl_mode": "REQUIRED"}})

# Specify the table name
table_name = "065122022_meta_info"

# Use pandas to read the table contents into a DataFrame
with engine.connect() as connection:
    df = pd.read_sql_table(table_name, connection)

df.index = pd.to_datetime(df['date'])
df['week_of_year'] = ((df['date'] - pd.to_datetime(df['date'].dt.year.astype(str) + '-01-01')).dt.days // 7) + 1
df.drop(columns=['date'], inplace=True)

pest_min = 9
pest_max = 45
pest_total = 760
pest_humidity = 65

day_degrees = []  # Create an empty list to store day_degree values
for index, row in df.iterrows():
    temp_min = max(row['daily_min'], pest_min)
    temp_max = min(row['daily_max'], pest_max)
    day_degree = max(0, (temp_min + temp_max) / 2 - pest_min)
    day_degrees.append(day_degree)  # Append each calculated value
df['day_degree'] = day_degrees

gens = []
day_degrees_values = df['day_degree'].values
cumulative_sum = 0
for i, value in enumerate(day_degrees_values):
    cumulative_sum += value  # Add current value to cumulative sum
    gens.append(cumulative_sum / pest_total)
df['gens'] = gens
df['gens'] = df['gens'] % 1


adj_gens = []
gens = df['gens'].values
avg_humidity = df['avg_hum'].values
for i in range(0, len(gens)):
    adj_gens.append(adjust_gens(i, gens, avg_humidity, pest_humidity))

# Convert and round each value to two decimal places
adj_gens = [round(float(value), 4) for value in adj_gens]

df['adj_gens'] = adj_gens

# with pd.option_context('display.max_rows', None):
#     print(df)
#
plt.figure(figsize=(15, 6))
plt.scatter(df.index, df['gens'], label="Generations", color="red")
plt.scatter(df.index, df['adj_gens'], label="Adjusted Generations", color="blue")

# Add title and labels
plt.title("Generations and Adjusted Generations Time")
plt.xlabel("Date")
plt.ylabel("Values")
plt.legend()
plt.show()
