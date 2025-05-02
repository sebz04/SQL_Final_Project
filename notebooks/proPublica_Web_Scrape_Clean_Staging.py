# nonprofit_webscrape_clean_staging.py

# %% 

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime

# %% 

# Load environment variables
load_dotenv()

# Set up PostgreSQL connection
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
pg_engine = create_engine(pg_conn_str)

# %% 

# Step 1: Read raw nonprofit financials
raw_financials_df = pd.read_sql_table(
    table_name='nonprofit_financials',
    con=pg_engine,
    schema='raw'
)

# %% 

# Step 2: Clean nonprofit financials data
financials_cleaned = raw_financials_df.copy()
financials_cleaned['ein'] = financials_cleaned['ein'].astype(str).str.zfill(9)

# Replace 'N/A' with NaN for all fields
financials_cleaned.replace('N/A', pd.NA, inplace=True)

# flag for category
financials_cleaned['has_category'] = financials_cleaned['category'].notna() 

# Clean year and convert to datetime format (optional)
financials_cleaned['year'] = pd.to_datetime(financials_cleaned['year'], format='%Y', errors='coerce')
financials_cleaned['year_int'] = financials_cleaned['year'].dt.year  # optional integer version for grouping

# Add a flag to indicate if financial data exists
financials_cleaned['has_financials'] = financials_cleaned['revenue'].notna()
financials_cleaned['revenue'] = pd.to_numeric(financials_cleaned['revenue'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce')
financials_cleaned['expenses'] = pd.to_numeric(financials_cleaned['expenses'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce')
financials_cleaned['net_income'] = pd.to_numeric(financials_cleaned['net_income'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce')
financials_cleaned['net_assets'] = pd.to_numeric(financials_cleaned['net_assets'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce')
financials_cleaned['cleaned_at'] = datetime.now()

# Clean percentages if needed (optional)

# %% 

# Step 3: Write to staging table
financials_cleaned.to_sql(
    name='staging_nonprofit_financials',
    con=pg_engine,
    schema='staging',
    if_exists='replace',
    index=False,
    chunksize=500,
    method='multi'
)

print(f"✅ Cleaned nonprofit financials data written to staging.staging_nonprofit_financials")

# ------------------------------------------------------

# %% 

# Step 4: Read raw executive compensation data
raw_comp_df = pd.read_sql_table(
    table_name='org_comp',
    con=pg_engine,
    schema='raw'
)

# %% 

# Step 5: Clean executive compensation data
comp_cleaned = raw_comp_df.copy()

# ✅ Replace 'N/A' with NULL-equivalent (Pandas NA)
comp_cleaned.replace('N/A', pd.NA, inplace=True)

# ✅ Clean EIN column
comp_cleaned['ein'] = comp_cleaned['ein'].astype(str).str.zfill(9)

# ✅ Remove dollar signs and convert to float (NULLs preserved)
for col in ['first_employee_compensation', 'second_employee_compensation', 'third_employee_compensation']:
    comp_cleaned[col] = pd.to_numeric(
        comp_cleaned[col].str.replace(r'[^0-9.]', '', regex=True),
        errors='coerce'
    )

# ✅ Add flag: does this org have at least one named employee?
comp_cleaned['has_employee'] = comp_cleaned['first_employee_name'].notna()
comp_cleaned['cleaned_at'] = datetime.now()

# %% 

# Step 6: Write to staging table
comp_cleaned.to_sql(
    name='staging_org_comp',
    con=pg_engine,
    schema='staging',
    if_exists='replace',
    index=False,
    chunksize=500,
    method='multi'
)

print(f"✅ Cleaned executive compensation data written to staging.staging_org_comp")