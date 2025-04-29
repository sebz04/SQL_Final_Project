# nonprofit_webscrape_clean_staging.py

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up PostgreSQL connection
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
pg_engine = create_engine(pg_conn_str)

# Step 1: Read raw nonprofit financials
raw_financials_df = pd.read_sql_table(
    table_name='nonprofit_financials',
    con=pg_engine,
    schema='raw'
)

# Step 2: Clean nonprofit financials data
financials_cleaned = raw_financials_df.copy()

# Example cleaning
financials_cleaned['ein'] = financials_cleaned['ein'].astype('Int64')
financials_cleaned['year'] = pd.to_numeric(financials_cleaned['year'], errors='coerce')
financials_cleaned['revenue'] = pd.to_numeric(financials_cleaned['revenue'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)
financials_cleaned['expenses'] = pd.to_numeric(financials_cleaned['expenses'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)
financials_cleaned['net_income'] = pd.to_numeric(financials_cleaned['net_income'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)
financials_cleaned['net_assets'] = pd.to_numeric(financials_cleaned['net_assets'].str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)

# Clean percentages if needed (optional)

# Step 3: Write to staging table
financials_cleaned.to_sql(
    name='staging_nonprofit_financials',
    con=pg_engine,
    schema='staging',
    if_exists='replace',
    index=False
)

print(f"✅ Cleaned nonprofit financials data written to staging.staging_nonprofit_financials")

# ------------------------------------------------------

# Step 4: Read raw executive compensation data
raw_comp_df = pd.read_sql_table(
    table_name='org_comp',
    con=pg_engine,
    schema='raw'
)

# Step 5: Clean executive compensation data
comp_cleaned = raw_comp_df.copy()

comp_cleaned['ein'] = comp_cleaned['ein'].astype('Int64')

for col in ['first_employee_compensation', 'second_employee_compensation', 'third_employee_compensation']:
    comp_cleaned[col] = pd.to_numeric(comp_cleaned[col].str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0)

# Step 6: Write to staging table
comp_cleaned.to_sql(
    name='staging_org_comp',
    con=pg_engine,
    schema='staging',
    if_exists='replace',
    index=False
)

print(f"✅ Cleaned executive compensation data written to staging.staging_org_comp")
