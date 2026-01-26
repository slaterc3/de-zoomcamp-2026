import pandas as pd
from sqlalchemy import create_engine

# 1. Download the Zones CSV
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
df = pd.read_csv(url)

# 2. Connect to Postgres (running on localhost:5432 via Docker Compose)
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# 3. Write to SQL
df.to_sql(name='zones', con=engine, if_exists='replace')

print("Zones table created successfully!")