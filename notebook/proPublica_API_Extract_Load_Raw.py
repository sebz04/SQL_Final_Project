
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
#Professor, if looking into this, I would recommend starting at line 263. 
#It's the first 100 nonprofits I found so I can see where I need to start cleaning data and how it would turn out
# %% 
from sqlalchemy import create_engine
!pip install mysqlclient
!pip install python-dotenv psycopg2-binary

# %% 
import requests  # Library to make HTTP requests (e.g., to access APIs)
import json  # Module to work with JSON data (parsing and formatting)
import pandas as pd  # Used for data manipulation and analysis
from urllib.parse import urlencode  # Function to encode query parameters in URLs
import os
from dotenv import load_dotenv
import psycopg2


# %%
search_url = 'https://projects.propublica.org/nonprofits/api/v2'
get_method = '/search.json?'
get_org_method = '/organizations/'

# Step 1: Define query parameters in a dictionary
params = {
    'q': 'ProPublica',
    #'state[id]': 'MA',  # Uncomment to filter by state
    #'c_code[id]': 3     # Uncomment to filter by 501(c)(3)
}

# Step 2: Encode them into a URL-friendly string
query_string = urlencode(params)

# Step 3: Combine with endpoint
response = requests.get(f'{search_url}{get_method}{query_string}')


# %%

data = response.json()
data
# %%



organization_search = {
    'ein': [],
    'name': [],
    'city': [],
    'state': [],
    'score': []
}

# %%

for org in data['organizations']:
    organization_search['ein'].append(org['ein'])
    organization_search['name'].append(org['name'])
    organization_search['city'].append(org['city'])
    organization_search['state'].append(org['state'])
    organization_search['score'].append(org['score'])


# %%
organization_search

# %%
data_frame = pd.DataFrame(organization_search)
data_frame 
# %%


for ein in organization_search['ein']:
    print(f'Fetching data for EIN: {ein}')
    # Step 4: Make a GET request to the API for each EIN
    org_details = requests.get(f'https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json').json()
    for filing in org_details['filings_with_data']:
        print({
            'year': filing['tax_prd_yr'],
            'totrevenue': filing.get('totrevenue'),
            'totfuncexpns': filing.get('totfuncexpns'),
            'totassetsend': filing.get('totassetsend'),
            'totliabend': filing.get('totliabend'),
            'pct_compnsatncurrofcr': filing.get('pct_compnsatncurrofcr')
        })
# %%

organization_info = {
    'ein': [],
    'name': [],
    'address': [],
    'city': [],
    'state': [],
    'zipcode': [],
    'subseccd': [],
    'ntee_code': [],
}

#Parameter to find city
params = {
    'q': '+Los+Angeles',
    'state[id]': 'CA'
}

for page_num in range(0, 400):  # Up to 10,000 results (400 pages * 25 per page)
    params['page'] = page_num
    query_string = urlencode(params)
    response = requests.get(f'{search_url}{get_method}{query_string}')
    
    if response.status_code != 200:
        break  # stop if something goes wrong
    
    data = response.json()
    
    for org in data['organizations']:
        if org['city'].lower() == 'los angeles':
            organization_search['ein'].append(org['ein'])
            organization_search['name'].append(org['name'])
            organization_search['city'].append(org['city'])
            organization_search['state'].append(org['state'])
            organization_search['score'].append(org['score'])

organization_search

# %%

for ein2 in organization_search['ein']:
    print(f'Fetching data for EIN: {ein2}')
    response2 = requests.get(f'https://projects.propublica.org/nonprofits/api/v2/organizations/{ein2}.json')
    data2 = response2.json()
    print(data2['organization'])
    #for i in data2['organization']:
       # print(i)
    #print(data2['organization'].keys()) 
    print("---------")
    organization_info['ein'].append(data2['organization']['ein'])
    organization_info['name'].append(data2['organization']['name']) 
    organization_info['address'].append(data2['organization']['address'])
    organization_info['city'].append(data2['organization']['city'])
    organization_info['state'].append(data2['organization']['state'])
    organization_info['zipcode'].append(data2['organization']['zipcode'])
    organization_info['subseccd'].append(data2['organization']['subsection_code'])
    organization_info['ntee_code'].append(data2['organization']['ntee_code'])
    #organization_info['updated'].append(data2['organization'].get('updated_at'))

organization_info
    

# %%
data_frame = pd.DataFrame(organization_info)
data_frame







# %%
#Finding All organizations in Los Angeles

search_url = 'https://projects.propublica.org/nonprofits/api/v2'
get_method = '/search.json?'
get_org_method = '/organizations/'

# Step 1: Define query parameters for Los Angeles in a dictionary
#Parameter to find city
params = {
    'q': '+Los+Angeles',
    'state[id]': 'CA'
}

# LIST to store EINs
ein_list = [] 

# %%

# Loop through up to 400 pages (25 results per page = 10,000 total)
for page in range(400):
    #Flips through each page
    print(f'Fetching page {page}...')
    params['page'] = page
    query_string = urlencode(params)
    response = requests.get(f'{search_url}{get_method}{query_string}')
    #gets response from API on specific page

    if response.status_code != 200:
        print(f'Stopped at page {page} due to error: {response.status_code}')
        break
    #checks if response is valid
    
    data = response.json()
    orgs = data.get('organizations', [])
    #gets the organizations from the response in the form of a list 
    
    if not orgs:
        print(f'No more results on page {page}. Ending loop.')
        break
    #checks if there are no more results on the page
    
    for org in orgs:
        if org.get('city', '').lower() == 'los angeles':
            ein_list.append(org['ein'])
            #adds the EIN to the list if the city is Los Angeles
    print(f'Found {len(ein_list)} EINs so far.')

# %%
print(f"✅ Total EINs collected: {len(ein_list)}")
print(ein_list[:5])  # Print first 5 EINs to see if came out good

# %%
#Now that I have the EINs of organizations in Los Angeles,
#I can use the EINs to get more information about each organization
# Step 2: Define the API endpoint and parameters

organization_info = {
    'ein': [],
    'name': [],
    'address': [],
    'city': [],
    'state': [],
    'zipcode': [],
    'subseccd': [],
    'ntee_code': [],
}

# Step 3: Make a GET request to the API for each EIN

for ein in ein_list:
    print(f'Fetching data for EIN: {ein}')
    response2 = requests.get(f'https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json')
    # call to get the organization object
    data2 = response2.json() 
    print("---------")
    organization_info['ein'].append(data2['organization']['ein'])
    organization_info['name'].append(data2['organization']['name']) 
    organization_info['address'].append(data2['organization']['address'])
    organization_info['city'].append(data2['organization']['city'])
    organization_info['state'].append(data2['organization']['state'])
    organization_info['zipcode'].append(data2['organization']['zipcode'])
    organization_info['subseccd'].append(data2['organization']['subsection_code'])
    organization_info['ntee_code'].append(data2['organization']['ntee_code'])



# %%
# Save into dataframe 
data_frame = pd.DataFrame(organization_info)
data_frame

# %%

##FOR FIRST 100 ORGS##

search_url = 'https://projects.propublica.org/nonprofits/api/v2'
get_method = '/search.json?'
get_org_method = '/organizations/'

# Step 1: Define query parameters for Los Angeles in a dictionary
params = {
    'q': '+Los+Angeles',
    'state[id]': 'CA'
}

# LIST to store EINs
ein_list = [] 

# Loop through up to 400 pages (25 results per page = 10,000 total)
for page in range(400):
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
        if len(ein_list) >= 100:
            break  # Stop once we hit 100 EINs

    print(f'Found {len(ein_list)} EINs so far.')
    if len(ein_list) >= 100:
        break  # Stop paginating once we have 100 EINs

# %%
print(f"✅ Total EINs collected: {len(ein_list)}")
print(ein_list[:5])  # Print first 5 EINs to see if came out good

# %%
# Now that I have the EINs of organizations in Los Angeles,
# I can use the EINs to get more information about each organization

organization_info = {
    'ein': [],
    'name': [],
    'address': [],
    'city': [],
    'state': [],
    'zipcode': [],
    'subseccd': [],
    'ntee_code': [],
    ##ADDING urls
    #'guidestar_url': [],
    #'ncss_url': [],
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
    organization_info['zipcode'].append(data2['organization']['zipcode'])
    organization_info['subseccd'].append(data2['organization']['subsection_code'])
    organization_info['ntee_code'].append(data2['organization']['ntee_code'])
    # ADDING URLS
    #organization_info['guidestar_url'].append(data2['organization']['guidestar_url'])
    #organization_info['ncss_url'].append(data2['organization'].get('ncss_url'))

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
table_name = 'nonprofits_100'
schema_name = 'sql_project'  # change this if you're using a custom schema

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

for ein in ein_list[:5]:  # First 5 EINs
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

# Preview result
financials_df

### NEXT STEPS ###
# Make these iterate through Zip Code so 
# it doesn't stop at 10,000 orgs --> adds more
# Possibly more efficient too, less brute force?






























