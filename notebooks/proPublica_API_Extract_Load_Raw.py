
#Notebook designed to extract data from csv

# %% 
# Download Dependencies
#!pip install mysqlclient
#!pip install python-dotenv psycopg2-binary
#!pip install geopy
# Uncomment when directly running it. Allows GitHub action to download through requriements.txt
from sqlalchemy import create_engine
import requests  # Library to make HTTP requests (e.g., to access APIs)
import json  # Module to work with JSON data (parsing and formatting)
import pandas as pd  # Used for data manipulation and analysis
from urllib.parse import urlencode  # Function to encode query parameters in URLs
import os
from dotenv import load_dotenv
import psycopg2
from urllib.parse import urlencode
import time  # To be polite and not overload Nominatim

# %%
# Get CSV Data along w

csv = pd.read_csv('../data/los_angeles_eins_202505031612.csv') # .. allows it to leave notebook towards data folder
csv
# %%
# Get EINS from CSV
ein_list = csv['EIN'].dropna().astype(str).str.zfill(9).tolist()
print(ein_list[:5])
# %%
# Fake EIN list to test
ein_2 = ein_list[:5]

# %%


organization_info = {
    'ein': [],
    'name': [],
    'address': [],
    'city': [],
    'state': [],
    'zipcode': [],
    'subseccd': [],
    'subseccd_category': [],
    'ntee_code': [],
    'ntee_code_major': [],
    'ntee_category': [],
    #'latitude': [],
    #'longitude': [],
}

ntee_lookup = {
    'A': 'Arts, Culture & Humanities',
    'B': 'Education',
    'C': 'Environment',
    'D': 'Animal-Related',
    'E': 'Health',
    'F': 'Mental Health & Crisis',
    'G': 'Diseases & Disorders',
    'H': 'Medical Research',
    'I': 'Crime, Legal & Justice',
    'J': 'Employment',
    'K': 'Food, Agriculture & Nutrition',
    'L': 'Housing & Shelter',
    'M': 'Public Safety & Disaster Relief',
    'N': 'Recreation & Sports',
    'O': 'Youth Development',
    'P': 'Human Services',
    'Q': 'International',
    'R': 'Civil Rights & Advocacy',
    'S': 'Community Improvement',
    'T': 'Philanthropy, Voluntarism & Grantmaking',
    'U': 'Science & Technology',
    'V': 'Social Science',
    'W': 'Public Policy & Research',
    'X': 'Religion-Related',
    'Y': 'Mutual & Membership Benefit',
    'Z': 'Unknown'
}

subseccd_lookup = {
    '3': 'Charitable Organizations (501(c)(3))',
    '2': 'Title-Holding Corporations (501(c)(2))',
    '4': 'Social Welfare Organizations (501(c)(4))',
    '5': 'Labor and Agricultural Organizations (501(c)(5))',
    '6': 'Business Leagues / Trade Associations (501(c)(6))',
    '7': 'Social and Recreational Clubs (501(c)(7))',
    '8': 'Fraternal Beneficiary Societies (501(c)(8))',
    '9': 'Voluntary Employee Beneficiary Associations (501(c)(9))',
    '10': 'Fraternal Societies (501(c)(10))',
    '11': 'Teachers’ Retirement Fund Associations (501(c)(11))',
    '12': 'Benevolent Life Insurance / Utility Companies (501(c)(12))',
    '13': 'Cemetery Companies (501(c)(13))',
    '14': 'State-Chartered Credit Unions (501(c)(14))',
    '15': 'Mutual Insurance Companies (501(c)(15))',
    '16': 'Cooperative Crop Finance (501(c)(16))',
    '17': 'Supplemental Unemployment Benefit Trusts (501(c)(17))',
    '18': 'Employee Funded Pension Trusts (501(c)(18))',
    '19': 'Veterans Organizations (501(c)(19))',
    '21': 'Black Lung Benefit Trusts (501(c)(21))',
    '22': 'Withdrawal Liability Payment Funds (501(c)(22))',
    '23': 'Veterans Organizations (post-1880 programs) (501(c)(23))',
    '25': 'Real Estate Title Holding Companies (501(c)(25))',
    '26': 'High-Risk Health Insurance Pools (501(c)(26))',
    '27': 'Workers Compensation Reinsurance (501(c)(27))',
    '28': 'National Railroad Retirement Trust (501(c)(28))',
    '92': 'Nonexempt Charitable Trusts (4947(a)(1))'
}

# ----- COPY START ----

# Step 3: Make a GET request to the API for each EIN

import time
import json

#for ein in ein_list:
for ein in ein_2:
    print(f'🔍 Fetching data for EIN: {ein}')
    url = f'https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json'
    
    try:
        response2 = requests.get(url)
        
        if response2.status_code == 429:
            print("⏳ Rate limit hit. Sleeping for 60 seconds...")
            time.sleep(60)
            response2 = requests.get(url)  # Retry once after delay
        
        if response2.status_code != 200:
            print(f"⚠️ Skipping EIN {ein} due to status code {response2.status_code}")
            continue

        try:
            data2 = response2.json()
        except json.JSONDecodeError:
            print(f"❌ Invalid JSON for EIN {ein}. Skipping.")
            continue

        print(data2)
        print("---------")

        organization_info['ein'].append(data2['organization']['ein'])
        organization_info['name'].append(data2['organization']['name']) 
        organization_info['address'].append(data2['organization']['address'])
        organization_info['city'].append(data2['organization']['city'])
        organization_info['state'].append(data2['organization']['state'])

        raw_zip = data2['organization'].get('zipcode', '')
        clean_zip = str(raw_zip)[:5] if raw_zip else '00000'
        organization_info['zipcode'].append(clean_zip)

        subseccd = str(data2['organization']['subsection_code'])
        organization_info['subseccd'].append(subseccd)
        organization_info['subseccd_category'].append(subseccd_lookup.get(subseccd, 'Unknown'))

        ntee_code = data2['organization'].get('ntee_code', '')
        organization_info['ntee_code'].append(ntee_code)
        ntee_major = ntee_code[0] if ntee_code else 'Z'
        organization_info['ntee_code_major'].append(ntee_major)
        ntee_category = ntee_lookup.get(ntee_major, 'Unknown')
        organization_info['ntee_category'].append(ntee_category)

    except Exception as e:
        print(f"🔥 Unexpected error for EIN {ein}: {e}")
        continue

# %%
# Create DataFrame from the organization_info dictionary
data_frame = pd.DataFrame(organization_info)
data_frame
# %%
# Necessary for Postgres data connection
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
pg_engine = create_engine(pg_conn_str)


# %%
# Save the DataFrame to PostgreSQL
load_dotenv()

table_name = 'nonprofits'
schema_name = 'raw'  # change this if you're using a custom schema

data_frame.to_sql(
    name=table_name,
    con=pg_engine,
    schema=schema_name,
    if_exists='replace',  # or 'append' if you're adding to an existing table
    index=False,
    chunksize=500,
    method='multi'
)
print(f"✅ DataFrame successfully loaded into table '{schema_name}.{table_name}'")
 

# %%
# Financials for each EIN in the first 5 organizations in Los Angeles

financial_data = []

for ein in ein_list:  # First 5 EINs
    print(f'Fetching data for EIN: {ein}')
    response = requests.get(f'https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json')
    
    if response.status_code != 200:
        print(f'Skipping EIN {ein} due to error {response.status_code}')
        continue

    org_details = response.json()
    
    filings = org_details.get('filings_with_data', [])
    filings_sorted = sorted(filings, key=lambda x: x.get('tax_prd_yr', 0), reverse=True)[:5]  # Top 5 recent

    for filing in filings_sorted:
        financial_data.append({
            'ein': str(ein).zfill(9),  # Ensure EIN is a string early (optional here too)
            'year': filing.get('tax_prd_yr'),
            'totrevenue': filing.get('totrevenue'),
            'totfuncexpns': filing.get('totfuncexpns'),
            'totassetsend': filing.get('totassetsend'),
            'totliabend': filing.get('totliabend'),
            'pct_compnsatncurrofcr': filing.get('pct_compnsatncurrofcr')
        })

# %%
# Convert to DataFrame
financials_df = pd.DataFrame(financial_data)
f_table = 'financial_history'
# %%
#sends info to Postgres server
pg_engine.dispose()  # Clean up the old one (optional but clean)
pg_engine = create_engine(pg_conn_str)

# %%
financials_df.to_sql(
    name=f_table,
    con=pg_engine,
    schema=schema_name,
    if_exists='replace',
    index=False,
    chunksize=500,
    method='multi'
)
print(f"✅ DataFrame successfully loaded into table '{schema_name}.{table_name}'")
 

# %%

# %%

# %%

# %%
