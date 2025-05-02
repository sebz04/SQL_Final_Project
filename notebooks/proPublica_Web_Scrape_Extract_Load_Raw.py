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
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# %%

# Set up Postgres connection
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
pg_engine = create_engine(pg_conn_str)

# %%

# Load EINs from your staging table
query = "SELECT ein FROM staging.staging_nonprofits"
ein_df = pd.read_sql(query, con=pg_engine)
ein_list = ein_df['ein'].dropna().astype(str).str.zfill(9).tolist()

# Containers for results
org_results = []
comp_results = []

# %%

# Scraper function (1 EIN at a time)
def scrape_ein(ein):
    org_row = {"ein": ein}
    comp_row = {"ein": ein}

    try:
        url = f"https://projects.propublica.org/nonprofits/organizations/{ein}"
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- Organization Info ---
        name_tags = soup.select('.text-hed-900')
        org_row['name'] = name_tags[1].text.strip() if len(name_tags) > 1 else 'N/A'

        category_tags = soup.select('.ntee-category')
        category = category_tags[0].text.strip() if category_tags else 'N/A'
        org_row['category'] = ' '.join(category.split())

        # Find latest valid filing section
        all_filings = soup.find_all('section', class_='single-filing-period')
        filing_block, filing_year = None, 'N/A'
        for section in all_filings:
            try:
                year = int(section.get('id', '').replace('filing', ''))
            except:
                continue
            temp_soup = BeautifulSoup(str(section), 'html.parser')
            if temp_soup.select_one('.row-revenue__number'):
                filing_block = section
                filing_year = year
                break
        org_row['year'] = filing_year

        # Parse the financial section
        soup2 = BeautifulSoup(str(filing_block), 'html.parser')
        get_text = lambda s: s.text.strip() if s else 'N/A'
        org_row['revenue'] = get_text(soup2.select_one('.row-revenue__number'))

        def extract_summary(label):
            el = soup2.find('div', string=label)
            return get_text(el.find_next_sibling('div')) if el else 'N/A'

        org_row['expenses'] = extract_summary('Expenses')
        org_row['net_income'] = extract_summary('Net Income')
        org_row['net_assets'] = extract_summary('Net Assets')

        # --- Compensation Info ---
        comp_table = soup2.find('table', class_='employees')
        if comp_table:
            rows = comp_table.find_all('tr', class_='employee-row')
            for i in range(3):
                if i < len(rows):
                    cols = rows[i].find_all('td')
                    name = cols[0].text.strip().split('\n')[0].strip()
                    title = cols[0].find('span')
                    title = title.text.strip('()') if title else 'N/A'
                    base_comp = cols[1].text.strip() if len(cols) > 1 else 'N/A'
                else:
                    name, title, base_comp = 'N/A', 'N/A', 'N/A'

                prefix = ['first', 'second', 'third'][i]
                comp_row[f'{prefix}_employee_name'] = name
                comp_row[f'{prefix}_employee_title'] = title
                comp_row[f'{prefix}_employee_compensation'] = base_comp
        else:
            for prefix in ['first', 'second', 'third']:
                comp_row[f'{prefix}_employee_name'] = 'N/A'
                comp_row[f'{prefix}_employee_title'] = 'N/A'
                comp_row[f'{prefix}_employee_compensation'] = 'N/A'

    except Exception as e:
        org_row['error'] = str(e)
        comp_row['error'] = str(e)

    return org_row, comp_row

# %%

# Run the scraping concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(scrape_ein, ein): ein for ein in ein_list}
    for future in as_completed(futures):
        ein = futures[future]  # get EIN associated with the future
        try:
            org_row, comp_row = future.result()
            org_results.append(org_row)
            comp_results.append(comp_row)
            print(f"✅ Fetched and processed EIN: {ein}")
        except Exception as e:
            print(f"❌ Error processing EIN {ein}: {e}")

# %%

# Convert results to DataFrames
org_df = pd.DataFrame(org_results)
comp_df = pd.DataFrame(comp_results)

# Save to Postgres
org_df.to_sql(
    name='nonprofit_financials',
    con=pg_engine,
    schema='raw',
    if_exists='replace',
    index=False,
    chunksize=500,
    method='multi'
)
print("✅ Organization data saved to raw.nonprofit_financials")

comp_df.to_sql(
    name='org_comp',
    con=pg_engine,
    schema='raw',
    if_exists='replace',
    index=False,
    chunksize=500,
    method='multi'
)
print("✅ Compensation data saved to raw.org_comp")


# %%

# %%

# %%

# %%

# %%

# %%



























