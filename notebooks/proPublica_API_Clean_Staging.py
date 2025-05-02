# nonprofit_api_clean_staging.py

# %%

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime


# Load environment variables
load_dotenv()

# %%

# Set up PostgreSQL connection
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
pg_engine = create_engine(pg_conn_str)

# %%

# Step 1: Read raw nonprofit data
raw_nonprofit_df = pd.read_sql_table(
    table_name='nonprofits',
    con=pg_engine,
    schema='raw'
)

# %%

# Step 2: Clean nonprofit data
nonprofit_cleaned = raw_nonprofit_df.copy()

# Example cleaning:
nonprofit_cleaned['ein'] = nonprofit_cleaned['ein'].astype(str).str.zfill(9)  # pad with leading zeroes if needed
nonprofit_cleaned['name'] = nonprofit_cleaned['name'].str.strip()
nonprofit_cleaned['address'] = nonprofit_cleaned['address'].fillna('Unknown').str.upper()
nonprofit_cleaned['city'] = nonprofit_cleaned['city'].fillna('Unknown').str.upper()
nonprofit_cleaned['state'] = nonprofit_cleaned['state'].fillna('CA')  # Assume CA
nonprofit_cleaned['zipcode'] = nonprofit_cleaned['zipcode'].fillna('00000').astype(str).str[:5]

# (Optional: more advanced address cleaning here)

# Missing NTEE codes
nonprofit_cleaned['ntee_code'] = nonprofit_cleaned['ntee_code'].fillna('Z99')  # ProPublica uses Z99 for missing NTEE codes
nonprofit_cleaned['ntee_code_major'] = nonprofit_cleaned['ntee_code_major'].fillna('Z')
nonprofit_cleaned['ntee_category'] = nonprofit_cleaned['ntee_category'].fillna('Unknown')
nonprofit_cleaned['cleaned_at'] = datetime.now()

# %%

# Step 3: Write to staging table
nonprofit_cleaned.to_sql(
    name='staging_nonprofits',
    con=pg_engine,
    schema='staging',
    if_exists='replace',
    index=False,
    chunksize=500,
    method='multi'
)

print(f"✅ Cleaned nonprofit data written to staging.staging_nonprofits_100")

# ------------------------------------------------------
# %%

# Step 4: Read raw financials data
raw_financial_df = pd.read_sql_table(
    table_name='financial_history',
    con=pg_engine,
    schema='raw'
)

# %%

# Step 5: Clean financials data
financials_cleaned = raw_financial_df.copy()

financials_cleaned['ein'] = financials_cleaned['ein'].astype(str).str.zfill(9)
financials_cleaned['year'] = financials_cleaned['year'].astype('Int64')
financials_cleaned['date_year'] = pd.to_datetime(
    financials_cleaned['year'].astype(str) + '-01-01',
    errors='coerce'
)
financials_cleaned['totrevenue'] = pd.to_numeric(financials_cleaned['totrevenue'], errors='coerce').fillna(0)
financials_cleaned['totfuncexpns'] = pd.to_numeric(financials_cleaned['totfuncexpns'], errors='coerce').fillna(0)
financials_cleaned['totassetsend'] = pd.to_numeric(financials_cleaned['totassetsend'], errors='coerce').fillna(0)
financials_cleaned['totliabend'] = pd.to_numeric(financials_cleaned['totliabend'], errors='coerce').fillna(0)
financials_cleaned['pct_compnsatncurrofcr'] = pd.to_numeric(financials_cleaned['pct_compnsatncurrofcr'], errors='coerce').fillna(0)
financials_cleaned['cleaned_at'] = datetime.now()

# %%

# Step 6: Write to staging table
financials_cleaned.to_sql(
    name='staging_financial_history',
    con=pg_engine,
    schema='staging',
    if_exists='replace',
    index=False,
    chunksize=500,
    method='multi'
)

print(f"✅ Cleaned financial data written to staging.staging_financial_history")

# %%
