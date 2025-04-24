#1 Use ProPublica API to find EIN
#2 Use EIN to inplement in url https://projects.propublica.org/nonprofits/organizations/{ein}
#3 Use BeautifulSoup to scrape the data
# %%
!pip install mysqlclient
!pip install sqlalchemy
!pip install python-dotenv psycopg2-binary
# %%
#import libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import psycopg2

# %% 

#I'm using an EIN list that I already uploaded into Postgres
#For the sake of my sleep, I'm using a set list. 
# Hopefully, by the time you see this, I'll commit a new change where it'll use info from my Postgres database to scrape based on EIN from the database itself


# %%

organization_detail = {
        "ein": [],
        "name": [],
        "category": [],
        "year": [],
        "revenue": [],
        "expenses": [],
        "net_income": [],
        "net_assets": [],
        "contributions": [],
        "contribution_perc": [],
        "program_service_revenue": [],
        "program_service_revenue_perc": [],
        "investment_income": [],
        "investment_income_perc": [],
        "bond_proceeds": [],
        "bond_proceeds_perc": [],
        "royalties": [],
        "royalties_perc": [],
        "rent_income": [],
        "rent_income_perc": [],
        "sales": [],
        "sales_perc": [],
        "net_inventory": [],
        "net_inventory_perc": [],
        "other_revenue": [],
        "other_revenue_perc": [],
        "executive_comp": [],
        "executive_comp_perc": [],
        "fundraising_fees": [],
        "fundraising_fees_perc": [],
        "salaries_wages": [],
        "salaries_wages_perc": []
    }

compensation_detail = {
        "ein": [],
        "first_employee_name": [],
        "first_employee_title": [],
        "first_employee_compensation": [],
        "second_employee_name": [],
        "second_employee_title": [],
        "second_employee_compensation": [],
        "third_employee_name": [],
        "third_employee_title": [],
        "third_employee_compensation": [],
}

# %%
#Setting up Postgres connection

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

# %%
#Gather ein from Postgres database
query = "SELECT ein FROM sql_project.nonprofits_100"
ein_df = pd.read_sql(query, con=pg_engine)

# ✅ Convert EINs to a flat list
einList = ein_df['ein'].dropna().astype(int).tolist()
print(f"📥 Retrieved {len(einList)} EINs from Postgres.")


# %%
# Testing with 100 LA nonprofits!

einList = [
    954444787, 823166229, 953273046, 952906949, 264286940, 954841936, 452390782, 863753196, 882495552, 912171518,
    953613187, 953895589, 833722687, 870709457, 800011266, 954120544, 883448557, 831017038, 813068173, 853467492,
    954266571, 931802179, 951690977, 953033333, 951696734, 953567895, 952408623, 952635019, 952096402, 950948160,
    954002138, 454998717, 953134049, 954862553, 953909218, 954700442, 953718119, 201819852, 951691288, 950951420,
    921018736, 833172348, 204637089, 954481955, 61692670, 205142259, 455434395, 262358338, 953921204, 812571475,
    952825348, 853685167, 237208448, 954547514, 952988883, 852023078, 954264361, 204555989, 10761875, 956048122,
    954174562, 956072782, 950734614, 475136561, 954374643, 474391440, 851912511, 953423223, 954589251, 460575192,
    475314677, 950947510, 953287926, 813986670, 953980495, 953471695, 861404052, 952701113, 953302452, 954379560,
    273332652, 833460767, 954412529, 851979554, 474482497, 270579751, 954694129, 320498349, 934493920, 861620575,
    270732846, 472185490, 842988087, 822069289, 461118356, 872882952, 463886104, 274434017, 850868163, 825105055
]
  # these are confirmed valid
  #ein list from the database and previous API call

for ein in einList:
    print(ein)

#ein = 951643334
#ein2 = 142007220
    url = f"https://projects.propublica.org/nonprofits/organizations/{ein}"
    # Make a GET request to the URL
    response = requests.get(url)
    response.text

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    soup
    
    name_tags = soup.select('.text-hed-900')
    name = name_tags[1].text.strip() if len(name_tags) > 1 else 'N/A'
    organization_detail['name'].append(name)
    organization_detail['ein'].append(ein)

    category_tags = soup.select('.ntee-category')
    category = category_tags[0].text.strip() if category_tags else 'N/A'
    category = ' '.join(category.split())  # remove \n, \t, etc.
    organization_detail['category'].append(category)
    # 

    # Step 1: Find all filings on record
    #Filings based on NEWEST filing
    all_filings = soup.find_all('section', class_='single-filing-period')

    # Step 2: Grab the first one — ProPublica orders them latest first
    if all_filings:
        first_section = all_filings[0]
        id_attr = first_section.get('id', '')

        try:
            filing_year = int(id_attr.replace('filing', ''))
        except ValueError:
            filing_year = 'N/A'

        organization_detail['year'].append(filing_year)
        filing_block = first_section
        soup2 = BeautifulSoup(str(filing_block), 'html.parser')

    else:
        organization_detail['year'].append('N/A')
        filing_block = None

    # --- Extracting Financial Data --- 

    soup2 = BeautifulSoup(str(filing_block), 'html.parser')

    # --- Summary Financials ---
    get_text = lambda s: s.text.strip() if s else 'N/A'
    revenue = get_text(soup2.select_one('.row-revenue__number'))

    def extract_summary(label):
        el = soup2.find('div', string=label)
        return get_text(el.find_next_sibling('div')) if el else 'N/A'

    expenses = extract_summary('Expenses')
    net_income = extract_summary('Net Income')
    net_assets = extract_summary('Net Assets')

    # --- Detailed Revenue Sources with Percentages ---
    revenue_sources = {}
    for row in soup2.select('table.revenue tbody tr'):
        cols = row.find_all('td')
        if len(cols) >= 3:
            source = cols[0].text.strip()
            amount = cols[1].text.strip()
            perc = cols[2].text.strip().split()[-1] if cols[2].text.strip() else 'N/A'
            revenue_sources[source] = {'amount': amount, 'percentage': perc}

    expenses_dict = {}
    for row in soup2.select('table.expenses tbody tr'):
        cols = row.find_all('td')
        if len(cols) >= 2:
            label = cols[0].text.strip()
            amount = cols[1].text.strip()
            perc = cols[2].text.strip().split()[-1] if len(cols) > 2 and cols[2].text.strip() else 'N/A'
            expenses_dict[label] = {'amount': amount, 'percentage': perc}

    # --- Append to organization_detail ---

    organization_detail['revenue'].append(revenue)
    organization_detail['expenses'].append(expenses)
    organization_detail['net_income'].append(net_income)
    organization_detail['net_assets'].append(net_assets)

    # Handle each known revenue source safely
    get_rev = lambda k, f: revenue_sources.get(k, {}).get(f, 'N/A')

    # --- Append to organization_detail ---
    get_exp = lambda k, f: expenses_dict.get(k, {}).get(f, 'N/A')

    organization_detail["executive_comp"].append(get_exp("Executive Compensation", "amount"))
    organization_detail["executive_comp_perc"].append(get_exp("Executive Compensation", "percentage"))

    organization_detail["fundraising_fees"].append(get_exp("Professional Fundraising Fees", "amount"))
    organization_detail["fundraising_fees_perc"].append(get_exp("Professional Fundraising Fees", "percentage"))

    organization_detail["salaries_wages"].append(get_exp("Other Salaries and Wages", "amount"))
    organization_detail["salaries_wages_perc"].append(get_exp("Other Salaries and Wages", "percentage"))

    organization_detail['contributions'].append(get_rev('Contributions', 'amount'))
    organization_detail['contribution_perc'].append(get_rev('Contributions', 'percentage'))

    organization_detail['program_service_revenue'].append(get_rev('Program Services', 'amount'))
    organization_detail['program_service_revenue_perc'].append(get_rev('Program Services', 'percentage'))

    organization_detail['investment_income'].append(get_rev('Investment Income', 'amount'))
    organization_detail['investment_income_perc'].append(get_rev('Investment Income', 'percentage'))

    organization_detail['bond_proceeds'].append(get_rev('Bond Proceeds', 'amount'))
    organization_detail['bond_proceeds_perc'].append(get_rev('Bond Proceeds', 'percentage'))

    organization_detail['royalties'].append(get_rev('Royalties', 'amount'))
    organization_detail['royalties_perc'].append(get_rev('Royalties', 'percentage'))

    organization_detail['rent_income'].append(get_rev('Rental Property Income', 'amount'))
    organization_detail['rent_income_perc'].append(get_rev('Rental Property Income', 'percentage'))

    organization_detail['sales'].append(get_rev('Sales of Assets', 'amount'))
    organization_detail['sales_perc'].append(get_rev('Sales of Assets', 'percentage'))

    organization_detail['net_inventory'].append(get_rev('Net Inventory Sales', 'amount'))
    organization_detail['net_inventory_perc'].append(get_rev('Net Inventory Sales', 'percentage'))

    organization_detail['other_revenue'].append(get_rev('Other Revenue', 'amount'))
    organization_detail['other_revenue_perc'].append(get_rev('Other Revenue', 'percentage'))

# %%
organization_detail

# %%
for key, value in organization_detail.items():
    print(f"{key}: {len(value)}")
# %%
dataFrame = pd.DataFrame(organization_detail)
dataFrame
#There are some issues with the data collected
#i intend on cleaning it in Python
#However, I want to demonstrate that I can load it into Postgres so I will do it in the following lines of code
# %%



# ✅ Define table name and schema (optional)
table_name = 'nonprofit_financials'
schema_name = 'sql_project'  # change this if you're using a custom schema

# ✅ Send DataFrame to Postgres
dataFrame.to_sql(
    name=table_name,
    con=pg_engine,
    schema=schema_name,
    if_exists='replace',  # or 'append' if you're adding to an existing table
    index=False
)
print(f"✅ DataFrame successfully loaded into table '{schema_name}.{table_name}'")





# %%

# %%

# %%

# %%

# %%

# %%



























