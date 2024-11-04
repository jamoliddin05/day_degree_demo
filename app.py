from datetime import datetime
from sqlalchemy import create_engine
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd

# You can generate a Token from the "Tokens Tab" in the UI
token = "QNO1_y5yUldC2hjjwx-1600KdVu0E4Hm_7HPhEdGjtj7mRAYhcv-R89_YKXEnqmCx3OQdhbZts9p6JPcsZkjoA=="
org = "a8f96954e8a2e4ad"
bucket = "oxus2"
url = "http://oxus2.amudar.io:9999"
stationID = "043112022"
field = "AirT"

client = InfluxDBClient(
    url=url,
    token=token,
    org=org
)

query_max = f'''
            from(bucket: "oxus2")
            |> range(start: -1y)
            |> filter(fn: (r) => r["stationID"] == "{stationID}")
            |> filter(fn: (r) => r["_field"] == "{field}")
            |> aggregateWindow(every: 1d, fn: max, createEmpty: false)
            |> yield(name: "daily_max")
        '''

query_min = f'''
            from(bucket: "oxus2")
            |> range(start: -1y)
            |> filter(fn: (r) => r["stationID"] == "{stationID}")
            |> filter(fn: (r) => r["_field"] == "{field}")
            |> aggregateWindow(every: 1d, fn: min, createEmpty: false)
            |> yield(name: "daily_min")
        '''

# Query daily max temperatures
tables_max = client.query_api().query(query_max, org=org)
results_max = []
for table in tables_max:
    for record in table.records:
        results_max.append({
            "time": record.get_time(),
            "max_temperature": record.get_value()
        })

# Query daily min temperatures
tables_min = client.query_api().query(query_min, org=org)
results_min = []
for table in tables_min:
    for record in table.records:
        results_min.append({
            "time": record.get_time(),
            "min_temperature": record.get_value()
        })

# Convert results to DataFrames
df_max = pd.DataFrame(results_max).set_index("time")
df_min = pd.DataFrame(results_min).set_index("time")

# Combine max and min temperatures into a single DataFrame
df = df_max.join(df_min)
df['date_new'] = pd.to_datetime(df.index)
df['date'] = pd.to_datetime(df['date_new'], format='%m%d%Y').dt.strftime('%Y-%m-%d')
df.index = df['date']
df.drop('date_new', axis=1, inplace=True)
df.drop('date', axis=1, inplace=True)

engine = create_engine('postgresql://localhost/day_degree_db')
df.to_sql(f'{stationID}_daily_temperatures', engine, if_exists='replace', index=True)

print("Data saved successfully to the database.")
