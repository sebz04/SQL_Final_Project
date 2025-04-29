
# %% 
''' 
Writing steps down to extract data from ProPublica API and load it into a raw data folder.
1. Import necessary libraries
*** NO NEED FOR Authorization code for this API ***
2. Define the API endpoint and parameters
3. Make a GET request to the API
4. Parse the JSON response
5. Print the organization details
6. Organize the data into a DataFrame
'''


from sqlalchemy import create_engine
!pip install mysqlclient
!pip install python-dotenv psycopg2-binary
!pip install geopy


# %% 
import requests  # Library to make HTTP requests (e.g., to access APIs)
import json  # Module to work with JSON data (parsing and formatting)
import pandas as pd  # Used for data manipulation and analysis
from urllib.parse import urlencode  # Function to encode query parameters in URLs
import os
from dotenv import load_dotenv
import psycopg2
from urllib.parse import urlencode
from geopy.geocoders import Nominatim
import time  # To be polite and not overload Nominatim
import re #Regular expressions --> when cleaning addresses, make it easier 


# %%

geolocator = Nominatim(user_agent="nonprofit_geocode")  
# uses API to get lat/long for each organization like BeautifulSoup 

#Geocode function to get latitude and longitude from address
def get_lat_lon(address, city, state, zipcode, is_po_box=False):
    if is_po_box:
        #if address is a PO Box, we need to use the zipcode and city
        query = f"{zipcode}, {city}, {state}"
    else:
        query = f"{address}, {city}, {state}, {zipcode}"
    
    try:
        location = geolocator.geocode(query)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except Exception as e:
        print(f"Error geocoding address {query}: {e}")
        return None, None
    
def clean_address(address):
    """
    Cleans nonprofit address for better geocoding:
    - Removes suite, floor, apartment, unit numbers
    - If PO BOX is detected, keeps only ZIP code later
    """
    if not address:
        return ''

    address = address.upper()

    # Handle P.O. Box case separately
    if 'PO BOX' in address:
        return address.split('PO BOX')[0].strip()

    # Remove common suite/unit/floor notations
    address = re.sub(r'\s+(STE|SUITE|UNIT|FLOOR|FL|APT|APARTMENT|BLDG)\s+\d+[A-Z]*', '', address)
    address = re.sub(r'\s+\d+(ST|ND|RD|TH)\s+FLOOR', '', address)  # Handle things like "49TH FLOOR"

    # Remove extra spaces that may appear after cleaning
    address = re.sub(r'\s+', ' ', address).strip()

    return address
# %%


# -- GOING through each ntee id -- 
# ----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------
#----------------------------------------------------------------



search_url = 'https://projects.propublica.org/nonprofits/api/v2'
get_method = '/search.json?'
get_org_method = '/organizations/'

# %%
# Go through each ntee id 
c_code_list = ['2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '23', '25', '26', '27', '28', '92']	

ein_list = []  

for c_c in c_code_list:

    print(c_c)
    
    params = {
    'q': '+Los+Angeles',
    'state[id]': 'CA',
    'c_code[id]': c_c
    }

    # LIST to store EINs
    

    # Loop through up to 400 pages (25 results per page = 10,000 total)
    #for page in range(400):
    for page in range(3):
        print(f'Fetching page {page}...')
        params['page'] = page
        query_string = urlencode(params)
        response = requests.get(f'{search_url}{get_method}{query_string}')

        if response.status_code != 200:
            print(f'Stopped at page {page} due to error: {response.status_code}')
            break

        data = response.json()
        orgs = data.get('organizations', [])

        if not orgs:
            print(f'No more results on page {page}. Ending loop.')
            break

        for org in orgs:
            if org.get('city', '').lower() == 'los angeles':
                ein_list.append(org['ein'])
            #if len(ein_list) >= 100:
                #break  # Stop once we hit 100 EINs

        print(f'Found {len(ein_list)} EINs so far.')
        if len(ein_list) >= 1000:
            break  # Stop paginating once we have 100 EINs

# %%
print(f"✅ Total EINs collected: {len(ein_list)}")
print(ein_list[:5])  # Print first 5 EINs to see if came out good

# %%
# Now that I have the EINs of organizations in Los Angeles,
# I can use the EINs to get more information about each organization
#ein_list = [952366826, 352215082, 472331261, 956047153]


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


# Step 3: Make a GET request to the API for each EIN

for ein in ein_list:
    print(f'Fetching data for EIN: {ein}')
    response2 = requests.get(f'https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json')
    data2 = response2.json()
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
    # original zip code --> organization_info['zipcode'].append(data2['organization']['zipcode'])
    # original subseccd code --> organization_info['subseccd'].append(data2['organization']['subsection_code'])
    subseccd = str(data2['organization']['subsection_code'])  # make sure it's a string
    organization_info['subseccd'].append(subseccd)
    # looking up the meaning
    organization_info['subseccd_category'].append(subseccd_lookup.get(subseccd, 'Unknown'))
    ntee_code = data2['organization'].get('ntee_code', '')
    organization_info['ntee_code'].append(ntee_code)
    ntee_major = ntee_code[0] if ntee_code else 'Z'
    organization_info['ntee_code_major'].append(ntee_major)
    ntee_category = ntee_lookup.get(ntee_major, 'Unknown')
    organization_info['ntee_category'].append(ntee_category)

        # Before appending address info
    #address = data2['organization']['address']
    #city = data2['organization']['city']
    #state = data2['organization']['state']
    #zipcode = clean_zip

    #cleaned_address = clean_address(address)

    # ✅ Check if original address had PO BOX (after cleaning)
    #is_po_box = 'PO BOX' in address.upper()

    # ✅ Then geocode
    #lat, lon = get_lat_lon(cleaned_address, city, state, zipcode, is_po_box)

    #organization_info['latitude'].append(lat)
    #organization_info['longitude'].append(lon)

    # Add a polite delay to avoid hammering Nominatim servers
    #time.sleep(1)

# %%
# Save into dataframe 
data_frame = pd.DataFrame(organization_info)
data_frame



# %%
# Put into Postgres server 

pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
pg_engine = create_engine(pg_conn_str)



# %% 
# Save the DataFrame to PostgreSQL
# ✅ Optional: load environment variables
load_dotenv()

# ✅ Define table name and schema (optional)
table_name = 'nonprofits'
schema_name = 'raw'  # change this if you're using a custom schema

# ✅ Send DataFrame to Postgres
data_frame.to_sql(
    name=table_name,
    con=pg_engine,
    schema=schema_name,
    if_exists='replace',  # or 'append' if you're adding to an existing table
    index=False
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
    
    for filing in org_details.get('filings_with_data', []):
        financial_data.append({
            'ein': ein,
            'year': filing.get('tax_prd_yr'),
            'totrevenue': filing.get('totrevenue'),
            'totfuncexpns': filing.get('totfuncexpns'),
            'totassetsend': filing.get('totassetsend'),
            'totliabend': filing.get('totliabend'),
            'pct_compnsatncurrofcr': filing.get('pct_compnsatncurrofcr')
        })

# Convert to DataFrame
financials_df = pd.DataFrame(financial_data)
f_table = 'financial_history'

# Preview result
financials_df
# %%
#sends info to Postgres server
financials_df.to_sql(
    name=f_table,
    con=pg_engine,
    schema=schema_name,
    if_exists='replace',  # or 'append' if you're adding to an existing table
    index=False
)
print(f"✅ DataFrame successfully loaded into table '{schema_name}.{table_name}'")
 
